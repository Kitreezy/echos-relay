// peer — собеседник для проверки echos, когда второго телефона нет.
//
// Подключается к релею как обычный клиент: проходит рукопожатие, попадает в
// присутствие, пишет сообщения, рисует на чужих стенах и отвечает своей. С
// точки зрения приложения это живой человек — по проводу отличить нельзя.
//
// Нужен потому, что почти всё в echos проверяется вдвоём, а второй телефон
// есть не всегда: у бесплатного аккаунта разработчика три устройства на год,
// и тратить их на проверку переписки жалко.
//
//	go run ./cmd/peer -name Борис
//	go run ./cmd/peer -name Борис -url wss://echos-relay.onrender.com/ws
//	go run ./cmd/peer -name Борис -fresh     # тот же человек, но другой ключ
//
// Дальше команды со стдина, «?» покажет список.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"echos-relay/relay"
)

type peer struct {
	name string
	me   identity
	conn *websocket.Conn

	mu sync.Mutex
	// room — кто сейчас в комнате, по отпечаткам.
	room map[string]string
	// wall — своя стена. Заглушка её помнит, иначе отвечать на просьбу
	// показать стену было бы нечем.
	wall []Stroke
	// target — кому адресованы команды. Пустой — не выбран.
	target string
}

func main() {
	url := flag.String("url", "ws://127.0.0.1:8080/ws", "адрес релея")
	name := flag.String("name", "Собеседник", "имя, под которым представиться")
	fresh := flag.Bool("fresh", false,
		"новый ключ вместо устойчивого — так выглядит тёзка, а не старый знакомый")
	flag.Parse()

	me := identityFor(*name)
	if *fresh {
		me = freshIdentity()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	p := &peer{name: *name, me: me, room: map[string]string{}}

	if err := p.connect(ctx, *url); err != nil {
		fmt.Println("не подключился:", err)
		os.Exit(1)
	}
	defer p.conn.Close(websocket.StatusNormalClosure, "")

	fmt.Printf("%s на связи, адрес %s\n", p.name, p.me.fingerprint())
	fmt.Println("«?» — список команд, «q» — выход")

	go p.listen(ctx)
	p.readCommands(ctx)
}

// MARK: - Соединение

func (p *peer) connect(ctx context.Context, url string) error {
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, url, nil)
	if err != nil {
		return err
	}
	p.conn = conn

	// Первым делом релей присылает случайную строку, и до ответа на неё нас
	// никто не обслуживает.
	for {
		envelope, err := p.read(dialCtx)
		if err != nil {
			return err
		}
		if envelope.Kind != relay.KindChallenge {
			continue
		}

		payload, err := p.me.helloPayload(envelope.Payload)
		if err != nil {
			return err
		}

		return p.send(dialCtx, relay.Envelope{
			Kind:    relay.KindHello,
			Sender:  p.name,
			Payload: payload,
		})
	}
}

func (p *peer) read(ctx context.Context) (relay.Envelope, error) {
	var envelope relay.Envelope

	_, data, err := p.conn.Read(ctx)
	if err != nil {
		return envelope, err
	}

	return envelope, json.Unmarshal(data, &envelope)
}

func (p *peer) send(ctx context.Context, envelope relay.Envelope) error {
	data, err := envelope.Encode()
	if err != nil {
		return err
	}

	return p.conn.Write(ctx, websocket.MessageBinary, data)
}

// MARK: - Входящее

func (p *peer) listen(ctx context.Context) {
	for {
		envelope, err := p.read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				fmt.Println("\nсвязь оборвалась:", err)
			}
			return
		}

		switch envelope.Kind {
		case relay.KindPresence:
			p.handlePresence(envelope)

		case relay.KindMessage:
			var message Message
			json.Unmarshal(envelope.Payload, &message)
			fmt.Printf("\n[%s] %s\n> ", p.nameOf(envelope.Sender), message.Text)

		case relay.KindTyping:
			var typing Typing
			json.Unmarshal(envelope.Payload, &typing)
			if typing.Type == "start" {
				fmt.Printf("\n%s печатает…\n> ", p.nameOf(envelope.Sender))
			}

		case relay.KindStroke:
			var stroke Stroke
			json.Unmarshal(envelope.Payload, &stroke)
			p.mu.Lock()
			p.wall = append(p.wall, stroke)
			count := len(p.wall)
			p.mu.Unlock()
			fmt.Printf("\n%s нарисовал на вашей стене, всего росчерков: %d\n> ",
				p.nameOf(envelope.Sender), count)

		case relay.KindWallRequest:
			p.answerWall(ctx, envelope.Sender)

		case relay.KindWallState:
			var strokes []Stroke
			json.Unmarshal(envelope.Payload, &strokes)
			fmt.Printf("\nстена %s: %d росчерков\n> ",
				p.nameOf(envelope.Sender), len(strokes))
		}
	}
}

func (p *peer) handlePresence(envelope relay.Envelope) {
	var participants []relay.Participant
	if json.Unmarshal(envelope.Payload, &participants) != nil {
		return
	}

	p.mu.Lock()
	p.room = map[string]string{}
	for _, participant := range participants {
		if participant.ID != p.me.fingerprint() {
			p.room[participant.ID] = participant.Name
		}
	}
	others := len(p.room)
	p.mu.Unlock()

	fmt.Printf("\nв комнате ещё %d, «who» покажет кто\n> ", others)
}

// answerWall отдаёт свою стену тому, кто спросил.
func (p *peer) answerWall(ctx context.Context, asker string) {
	p.mu.Lock()
	wall := append([]Stroke{}, p.wall...)
	p.mu.Unlock()

	payload, err := json.Marshal(wall)
	if err != nil {
		return
	}

	p.send(ctx, relay.Envelope{
		Kind:      relay.KindWallState,
		Recipient: asker,
		Payload:   payload,
	})

	fmt.Printf("\n%s спросил стену, отдал %d росчерков\n> ",
		p.nameOf(asker), len(wall))
}

// MARK: - Команды

func (p *peer) readCommands(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}

		command, rest, _ := strings.Cut(strings.TrimSpace(scanner.Text()), " ")
		rest = strings.TrimSpace(rest)

		switch command {
		case "", "?", "help":
			p.printHelp()

		case "who":
			p.printRoom()

		case "to":
			p.setTarget(rest)

		case "say":
			p.say(ctx, rest)

		case "typing":
			p.typing(ctx, rest)

		case "draw":
			p.draw(ctx)

		case "wall":
			p.askWall(ctx)

		case "mywall":
			p.printOwnWall()

		case "q", "quit", "exit":
			return

		default:
			fmt.Println("не знаю такой команды, «?» покажет список")
		}

		fmt.Print("> ")
	}
}

func (p *peer) printHelp() {
	fmt.Println(`  who            кто в комнате
  to <имя>       выбрать собеседника (можно частью имени или отпечатка)
  say <текст>    написать выбранному
  typing on|off  показать, что печатаете
  draw           нарисовать росчерк на стене выбранного
  wall           попросить у выбранного его стену
  mywall         что нарисовали на вашей стене
  q              выход`)
}

func (p *peer) printRoom() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.room) == 0 {
		fmt.Println("  никого")
		return
	}

	for id, name := range p.room {
		mark := " "
		if id == p.target {
			mark = "→"
		}
		fmt.Printf("%s %s (%s)\n", mark, name, id)
	}
}

// setTarget ищет по имени или отпечатку, годится и часть.
func (p *peer) setTarget(query string) {
	if query == "" {
		fmt.Println("  кого выбрать? например: to Алиса")
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var matches []string
	for id, name := range p.room {
		if strings.Contains(strings.ToLower(name), strings.ToLower(query)) ||
			strings.HasPrefix(id, query) {
			matches = append(matches, id)
		}
	}

	switch len(matches) {
	case 0:
		fmt.Println("  никого похожего в комнате нет")

	case 1:
		p.target = matches[0]
		fmt.Printf("  теперь пишем: %s (%s)\n", p.room[p.target], p.target)

	default:
		// Тёзки — обычное дело, ради них всё и затевалось.
		fmt.Println("  таких несколько, выберите по отпечатку:")
		for _, id := range matches {
			fmt.Printf("    %s (%s)\n", p.room[id], id)
		}
	}
}

func (p *peer) currentTarget() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.target == "" {
		fmt.Println("  сначала выберите собеседника: to <имя>")
		return "", false
	}

	return p.target, true
}

func (p *peer) say(ctx context.Context, text string) {
	if text == "" {
		fmt.Println("  что сказать-то?")
		return
	}

	target, ok := p.currentTarget()
	if !ok {
		return
	}

	payload, _ := json.Marshal(Message{
		ID:         strings.ToUpper(newUUID()),
		Text:       text,
		SenderName: p.name,
		Timestamp:  float64(time.Now().UnixNano()) / 1e9,
	})

	p.send(ctx, relay.Envelope{
		Kind:      relay.KindMessage,
		Recipient: target,
		Payload:   payload,
	})
}

func (p *peer) typing(ctx context.Context, state string) {
	target, ok := p.currentTarget()
	if !ok {
		return
	}

	kind := "start"
	if state == "off" || state == "stop" {
		kind = "stop"
	}

	payload, _ := json.Marshal(Typing{
		Type:      kind,
		PeerName:  p.name,
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
	})

	p.send(ctx, relay.Envelope{
		Kind:      relay.KindTyping,
		Recipient: target,
		Payload:   payload,
	})
}

// draw рисует диагональ через всю стену.
//
// Одной точки мало: приложение считает такой росчерк случайным касанием и
// выбрасывает его — ровно так же, как выбросило бы палец, задевший экран.
func (p *peer) draw(ctx context.Context) {
	target, ok := p.currentTarget()
	if !ok {
		return
	}

	stroke := newStroke(p.me.fingerprint(), diagonal())
	payload, _ := json.Marshal(stroke)

	p.send(ctx, relay.Envelope{
		Kind:      relay.KindStroke,
		Recipient: target,
		Payload:   payload,
	})

	fmt.Printf("  нарисовал у %s\n", p.nameOf(target))
}

func (p *peer) askWall(ctx context.Context) {
	target, ok := p.currentTarget()
	if !ok {
		return
	}

	p.send(ctx, relay.Envelope{Kind: relay.KindWallRequest, Recipient: target})
	fmt.Printf("  спросил стену у %s\n", p.nameOf(target))
}

func (p *peer) printOwnWall() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.wall) == 0 {
		fmt.Println("  на вашей стене пусто")
		return
	}

	for _, stroke := range p.wall {
		fmt.Printf("  %s — %d точек\n", p.nameOf(stroke.Author), len(stroke.Points))
	}
}

// nameOf переводит отпечаток в имя, если такой человек в комнате.
func (p *peer) nameOf(fingerprint string) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if name, known := p.room[fingerprint]; known {
		return name
	}

	if fingerprint == p.me.fingerprint() {
		return p.name
	}

	return fingerprint
}
