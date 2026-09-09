package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"echos-relay/config"
	"echos-relay/handlers"
	"echos-relay/relay"
)

// startRelay поднимает релей на случайном порту.
func startRelay(t *testing.T) string {
	t.Helper()

	cfg := config.Load()
	cfg.PingInterval = 100 * time.Millisecond
	cfg.PongTimeout = time.Second

	ws := &handlers.WS{Hub: relay.NewHub(), Cfg: cfg}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /ws", ws.Serve)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close(websocket.StatusNormalClosure, "") })

	return conn
}

func send(t *testing.T, conn *websocket.Conn, envelope relay.Envelope) {
	t.Helper()

	data, err := envelope.Encode()
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}

// receiveKind читает до первого конверта нужного типа.
func receiveKind(t *testing.T, conn *websocket.Conn, kind relay.Kind) relay.Envelope {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read failed while waiting for %s: %v", kind, err)
		}

		envelope, err := relay.Decode(data)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		if envelope.Kind == kind {
			return envelope
		}
	}
}

func hello(name string) relay.Envelope {
	return relay.Envelope{Kind: relay.KindHello, Sender: name}
}

func TestPresenceListsEveryoneWhoIntroducedThemselves(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)

	send(t, alice, hello("Alice"))
	send(t, bob, hello("Bob"))

	envelope := receiveKind(t, alice, relay.KindPresence)

	var names []string
	if err := json.Unmarshal(envelope.Payload, &names); err != nil {
		t.Fatalf("presence payload is not a list of names: %v", err)
	}

	// Список приходит дважды — после каждого hello. Ждём тот, где оба.
	for len(names) < 2 {
		envelope = receiveKind(t, alice, relay.KindPresence)
		if err := json.Unmarshal(envelope.Payload, &names); err != nil {
			t.Fatalf("presence payload is not a list of names: %v", err)
		}
	}

	if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
		t.Fatalf("expected [Alice Bob], got %v", names)
	}
}

func TestMessageReachesTheOtherClient(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)

	send(t, alice, hello("Alice"))
	send(t, bob, hello("Bob"))
	receiveKind(t, bob, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:    relay.KindMessage,
		Sender:  "Alice",
		Payload: []byte(`{"text":"привет"}`),
	})

	envelope := receiveKind(t, bob, relay.KindMessage)

	if string(envelope.Payload) != `{"text":"привет"}` {
		t.Fatalf("payload mangled: %s", envelope.Payload)
	}
}

// Имя в конверте — то, чем клиент назвался при подключении, а не то,
// что он написал в поле sender.
func TestSenderIsStampedByServer(t *testing.T) {
	url := startRelay(t)

	mallory := dial(t, url)
	bob := dial(t, url)

	send(t, mallory, hello("Mallory"))
	send(t, bob, hello("Bob"))
	receiveKind(t, bob, relay.KindPresence)

	send(t, mallory, relay.Envelope{
		Kind:    relay.KindMessage,
		Sender:  "Alice", // выдаёт себя за другого
		Payload: []byte(`{}`),
	})

	envelope := receiveKind(t, bob, relay.KindMessage)

	if envelope.Sender != "Mallory" {
		t.Fatalf("expected Mallory, got %q", envelope.Sender)
	}
}

func TestMessageFromUnintroducedClientIsIgnored(t *testing.T) {
	url := startRelay(t)

	stranger := dial(t, url)
	bob := dial(t, url)

	send(t, bob, hello("Bob"))
	receiveKind(t, bob, relay.KindPresence)

	// Незнакомец пишет, не представившись.
	send(t, stranger, relay.Envelope{
		Kind:    relay.KindMessage,
		Sender:  "Stranger",
		Payload: []byte(`{}`),
	})

	// Через секунду Bob не должен получить ничего.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, _, err := bob.Read(ctx); err == nil {
		t.Fatal("message from unintroduced client was relayed")
	}
}

func TestSenderDoesNotReceiveItsOwnMessage(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	send(t, alice, hello("Alice"))
	receiveKind(t, alice, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:    relay.KindMessage,
		Sender:  "Alice",
		Payload: []byte(`{}`),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, _, err := alice.Read(ctx); err == nil {
		t.Fatal("message echoed back to sender")
	}
}

func TestLeavingUpdatesPresence(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)

	send(t, alice, hello("Alice"))
	send(t, bob, hello("Bob"))
	receiveKind(t, alice, relay.KindPresence)

	bob.Close(websocket.StatusNormalClosure, "")

	for {
		envelope := receiveKind(t, alice, relay.KindPresence)

		var names []string
		if err := json.Unmarshal(envelope.Payload, &names); err != nil {
			t.Fatalf("presence payload is not a list of names: %v", err)
		}

		if len(names) == 1 && names[0] == "Alice" {
			return
		}
	}
}

func TestHealth(t *testing.T) {
	url := startRelay(t)
	healthURL := strings.Replace(strings.Replace(url, "ws://", "http://", 1), "/ws", "/health", 1)

	response, err := http.Get(healthURL)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.StatusCode)
	}
}

// Адресная доставка: росчерк предназначен одному и остальных не касается.
func TestAddressedEnvelopeReachesOnlyRecipient(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)
	carol := dial(t, url)

	send(t, alice, hello("Alice"))
	send(t, bob, hello("Bob"))
	send(t, carol, hello("Carol"))
	receiveKind(t, carol, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:      relay.KindStroke,
		Sender:    "Alice",
		Recipient: "Bob",
		Payload:   []byte(`{"points":[]}`),
	})

	// Bob получает
	envelope := receiveKind(t, bob, relay.KindStroke)
	if envelope.Sender != "Alice" {
		t.Fatalf("expected Alice, got %q", envelope.Sender)
	}

	// Carol — нет
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		_, data, err := carol.Read(ctx)
		if err != nil {
			return // тишина — то что нужно
		}

		decoded, err := relay.Decode(data)
		if err == nil && decoded.Kind == relay.KindStroke {
			t.Fatal("addressed stroke leaked to a third party")
		}
	}
}

// Конверт без получателя ведёт себя как раньше — уходит всем.
func TestEnvelopeWithoutRecipientStillBroadcasts(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)
	carol := dial(t, url)

	send(t, alice, hello("Alice"))
	send(t, bob, hello("Bob"))
	send(t, carol, hello("Carol"))
	receiveKind(t, carol, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:    relay.KindMessage,
		Sender:  "Alice",
		Payload: []byte(`{}`),
	})

	receiveKind(t, bob, relay.KindMessage)
	receiveKind(t, carol, relay.KindMessage)
}

// Получателя нет на релее — конверт не должен уйти кому-то другому.
func TestEnvelopeForAbsentRecipientIsDropped(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)

	send(t, alice, hello("Alice"))
	send(t, bob, hello("Bob"))
	receiveKind(t, bob, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:      relay.KindStroke,
		Sender:    "Alice",
		Recipient: "Nobody",
		Payload:   []byte(`{"points":[]}`),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		_, data, err := bob.Read(ctx)
		if err != nil {
			return
		}

		decoded, err := relay.Decode(data)
		if err == nil && decoded.Kind == relay.KindStroke {
			t.Fatal("stroke for an absent recipient was delivered to someone else")
		}
	}
}
