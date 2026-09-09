package handlers

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"echos-relay/config"
	"echos-relay/relay"
)

type WS struct {
	Hub *relay.Hub
	Cfg config.Config
}

func (h *WS) Serve(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Нативный клиент заголовок Origin не шлёт, а браузеров у нас нет —
		// проверять его нечего. Кто подключился, решает не заголовок, а
		// подпись в hello.
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Printf("handshake failed: %v", err)
		return
	}

	conn.SetReadLimit(h.Cfg.MaxMessageBytes)

	nonce, err := relay.NewNonce()
	if err != nil {
		log.Printf("nonce failed: %v", err)
		conn.Close(websocket.StatusInternalError, "nonce failed")
		return
	}

	client := h.Hub.Add()
	client.Nonce = nonce
	defer h.Hub.Remove(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go h.write(ctx, conn, client)
	go h.keepAlive(ctx, conn, client)
	go h.dropIfSilent(ctx, conn, client)

	// Вызов уходит первым: клиент ждёт его, чтобы было что подписать.
	if challenge, err := relay.ChallengeEnvelope(nonce).Encode(); err == nil {
		client.Send <- challenge
	}

	h.read(ctx, conn, client)

	conn.Close(websocket.StatusNormalClosure, "")
}

// read — единственное место, где разбираются входящие конверты.
func (h *WS) read(ctx context.Context, conn *websocket.Conn, client *relay.Client) {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if !isExpectedClose(err) {
				log.Printf("read failed: %v", err)
			}
			return
		}

		envelope, err := relay.Decode(data)
		if err != nil {
			log.Printf("bad envelope: %v", err)
			continue
		}

		switch envelope.Kind {
		case relay.KindHello:
			if client.HasIntroduced() {
				continue // уже представился, второй раз не нужно
			}

			if !h.authenticate(conn, client, envelope) {
				return
			}

			log.Printf("'%s' joined (%s)", client.Name(), client.Fingerprint())
			h.Hub.BroadcastPresence()

		case relay.KindMessage, relay.KindTyping, relay.KindStroke:
			if !client.HasIntroduced() {
				continue // не представился — не обслуживаем
			}

			// Отправителя проставляем сами: в конверте от клиента может
			// стоять чужое имя.
			stamped, err := envelope.StampedBy(client.Name()).Encode()
			if err != nil {
				log.Printf("encode failed: %v", err)
				continue
			}

			// Адресный конверт уходит одному. Остальных он не касается:
			// росчерк на стене Bob — не дело Carol.
			if envelope.Recipient != "" {
				if !h.Hub.RelayTo(stamped, envelope.Recipient, client) {
					log.Printf("'%s' is not here, dropping %s from '%s'",
						envelope.Recipient, envelope.Kind, client.Name())
				}
				continue
			}

			h.Hub.Relay(stamped, client)

		case relay.KindPresence, relay.KindChallenge:
			// И то и другое рассылает только сервер.
		}
	}
}

// authenticate проверяет hello: подпись под выданным nonce и право на имя.
//
// Отказ означает разрыв соединения, а не пропуск конверта. Клиент, который
// не смог подтвердить имя, не станет обслуживаемым позже — держать его на
// линии не за чем, а закрытие он увидит сразу.
func (h *WS) authenticate(conn *websocket.Conn, client *relay.Client, envelope relay.Envelope) bool {
	if envelope.Sender == "" {
		conn.Close(websocket.StatusPolicyViolation, "empty name")
		return false
	}

	hello, err := relay.DecodeHello(envelope.Payload)
	if err != nil {
		log.Printf("bad hello from '%s': %v", envelope.Sender, err)
		conn.Close(websocket.StatusPolicyViolation, "bad hello")
		return false
	}

	if !relay.VerifySignature(hello.PublicKey, client.Nonce, hello.Signature) {
		log.Printf("'%s' failed the challenge", envelope.Sender)
		conn.Close(websocket.StatusPolicyViolation, "bad signature")
		return false
	}

	if err := h.Hub.Claim(envelope.Sender, hello.PublicKey, client); err != nil {
		log.Printf("'%s' rejected: %v", envelope.Sender, err)
		conn.Close(websocket.StatusPolicyViolation, err.Error())
		return false
	}

	return true
}

// dropIfSilent закрывает соединение, если клиент так и не представился.
//
// Без этого любой открытый сокет висел бы до бесконечности: неподтверждённых
// мы не пингуем ответом и ничего им не шлём, так что сами они не отвалятся.
func (h *WS) dropIfSilent(ctx context.Context, conn *websocket.Conn, client *relay.Client) {
	timer := time.NewTimer(h.Cfg.AuthTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
		if !client.HasIntroduced() {
			log.Printf("client %d did not introduce itself, closing", client.ID)
			conn.Close(websocket.StatusPolicyViolation, "no hello")
		}
	}
}

// write — единственный писатель в это соединение.
func (h *WS) write(ctx context.Context, conn *websocket.Conn, client *relay.Client) {
	for {
		select {
		case <-ctx.Done():
			return

		case data, ok := <-client.Send:
			if !ok {
				return
			}

			writeCtx, cancel := context.WithTimeout(ctx, h.Cfg.PongTimeout)
			err := conn.Write(writeCtx, websocket.MessageBinary, data)
			cancel()

			if err != nil {
				if !isExpectedClose(err) {
					log.Printf("write failed: %v", err)
				}
				conn.Close(websocket.StatusInternalError, "write failed")
				return
			}
		}
	}
}

// keepAlive пингует клиента сам.
//
// Клиент пингует нас со своей стороны, но полагаться на это нельзя: если он
// уснул в фоне или его выкинул NAT, мы узнаем об этом только когда попробуем
// что-то отправить. А ещё прокси Render закрывает соединение после минуты
// тишины — трафик должен идти в обе стороны.
func (h *WS) keepAlive(ctx context.Context, conn *websocket.Conn, client *relay.Client) {
	ticker := time.NewTicker(h.Cfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, h.Cfg.PongTimeout)
			err := conn.Ping(pingCtx)
			cancel()

			if err != nil {
				log.Printf("'%s' did not answer ping, closing", client.Name())
				conn.Close(websocket.StatusPolicyViolation, "ping timeout")
				return
			}
		}
	}
}

// isExpectedClose отличает штатный уход клиента от настоящей ошибки:
// иначе лог забивается сообщениями о каждом закрытии вкладки.
func isExpectedClose(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}

	switch websocket.CloseStatus(err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway, websocket.StatusNoStatusRcvd:
		return true
	default:
		return false
	}
}
