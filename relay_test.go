package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
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
	cfg.AuthTimeout = 500 * time.Millisecond

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

// keyFor — устойчивая пара ключей на имя.
//
// В тестах «тот же Alice» должен приходить с тем же ключом, что и в прошлый
// раз, иначе переподключение выглядело бы как попытка занять чужое имя.
func keyFor(name string) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte(name))
	private := ed25519.NewKeyFromSeed(seed[:])
	return private.Public().(ed25519.PublicKey), private
}

// fingerprintOf — адрес, который релей выведет из ключа с этим именем.
func fingerprintOf(keyName string) string {
	public, _ := keyFor(keyName)
	return relay.Fingerprint(public)
}

// presenceOf читает список присутствующих.
func presenceOf(t *testing.T, conn *websocket.Conn) []relay.Participant {
	t.Helper()

	envelope := receiveKind(t, conn, relay.KindPresence)

	var participants []relay.Participant
	if err := json.Unmarshal(envelope.Payload, &participants); err != nil {
		t.Fatalf("presence payload is not a list of participants: %v", err)
	}

	return participants
}

// introduce проходит рукопожатие целиком: дожидается вызова и отвечает
// подписью под ним.
func introduce(t *testing.T, conn *websocket.Conn, name string) {
	t.Helper()

	public, private := keyFor(name)
	introduceWithKey(t, conn, name, public, private)
}

func introduceWithKey(t *testing.T, conn *websocket.Conn, name string,
	public ed25519.PublicKey, private ed25519.PrivateKey) {
	t.Helper()

	challenge := receiveKind(t, conn, relay.KindChallenge)

	payload, err := json.Marshal(relay.HelloPayload{
		PublicKey: public,
		Signature: ed25519.Sign(private, challenge.Payload),
	})
	if err != nil {
		t.Fatalf("hello payload failed: %v", err)
	}

	send(t, conn, relay.Envelope{Kind: relay.KindHello, Sender: name, Payload: payload})
}

func TestPresenceListsEveryoneWhoIntroducedThemselves(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)

	introduce(t, alice, "Alice")
	introduce(t, bob, "Bob")

	// Список приходит дважды — после каждого hello. Ждём тот, где оба.
	participants := presenceOf(t, alice)
	for len(participants) < 2 {
		participants = presenceOf(t, alice)
	}

	names := map[string]string{}
	for _, participant := range participants {
		names[participant.ID] = participant.Name
	}

	if names[fingerprintOf("Alice")] != "Alice" || names[fingerprintOf("Bob")] != "Bob" {
		t.Fatalf("expected Alice and Bob with their own fingerprints, got %v", participants)
	}
}

func TestMessageReachesTheOtherClient(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	bob := dial(t, url)

	introduce(t, alice, "Alice")
	introduce(t, bob, "Bob")
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

// Отправитель в конверте — отпечаток ключа, которым клиент подтвердил
// подключение, а не то, что он написал в поле sender.
func TestSenderIsStampedByServer(t *testing.T) {
	url := startRelay(t)

	mallory := dial(t, url)
	bob := dial(t, url)

	introduce(t, mallory, "Mallory")
	introduce(t, bob, "Bob")
	receiveKind(t, bob, relay.KindPresence)

	send(t, mallory, relay.Envelope{
		Kind:    relay.KindMessage,
		Sender:  fingerprintOf("Alice"), // выдаёт себя за другого
		Payload: []byte(`{}`),
	})

	envelope := receiveKind(t, bob, relay.KindMessage)

	if envelope.Sender != fingerprintOf("Mallory") {
		t.Fatalf("expected Mallory's fingerprint, got %q", envelope.Sender)
	}
}

func TestMessageFromUnintroducedClientIsIgnored(t *testing.T) {
	url := startRelay(t)

	stranger := dial(t, url)
	bob := dial(t, url)

	introduce(t, bob, "Bob")
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
	introduce(t, alice, "Alice")
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

	introduce(t, alice, "Alice")
	introduce(t, bob, "Bob")
	receiveKind(t, alice, relay.KindPresence)

	bob.Close(websocket.StatusNormalClosure, "")

	for {
		participants := presenceOf(t, alice)

		if len(participants) == 1 && participants[0].ID == fingerprintOf("Alice") {
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

	introduce(t, alice, "Alice")
	introduce(t, bob, "Bob")
	introduce(t, carol, "Carol")
	receiveKind(t, carol, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:      relay.KindStroke,
		Sender:    "Alice",
		Recipient: fingerprintOf("Bob"),
		Payload:   []byte(`{"points":[]}`),
	})

	// Bob получает
	envelope := receiveKind(t, bob, relay.KindStroke)
	if envelope.Sender != fingerprintOf("Alice") {
		t.Fatalf("expected Alice's fingerprint, got %q", envelope.Sender)
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

	introduce(t, alice, "Alice")
	introduce(t, bob, "Bob")
	introduce(t, carol, "Carol")
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

	introduce(t, alice, "Alice")
	introduce(t, bob, "Bob")
	receiveKind(t, bob, relay.KindPresence)

	send(t, alice, relay.Envelope{
		Kind:      relay.KindStroke,
		Sender:    "Alice",
		Recipient: fingerprintOf("Nobody"),
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

func TestUnsignedHelloIsRejected(t *testing.T) {
	url := startRelay(t)

	impostor := dial(t, url)
	receiveKind(t, impostor, relay.KindChallenge)

	// Имя есть, подписи нет.
	send(t, impostor, relay.Envelope{Kind: relay.KindHello, Sender: "Alice"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, _, err := impostor.Read(ctx); err == nil {
		t.Fatal("client without a signature stayed connected")
	}
}

func TestSignatureFromAnotherChallengeIsRejected(t *testing.T) {
	url := startRelay(t)

	// Подпись настоящая, но сделана не под тем, что выдал сервер.
	first := dial(t, url)
	stolen := receiveKind(t, first, relay.KindChallenge)

	second := dial(t, url)
	receiveKind(t, second, relay.KindChallenge)

	public, private := keyFor("Alice")
	payload, err := json.Marshal(relay.HelloPayload{
		PublicKey: public,
		Signature: ed25519.Sign(private, stolen.Payload),
	})
	if err != nil {
		t.Fatalf("hello payload failed: %v", err)
	}

	send(t, second, relay.Envelope{Kind: relay.KindHello, Sender: "Alice", Payload: payload})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, _, err := second.Read(ctx); err == nil {
		t.Fatal("signature made for another challenge was accepted")
	}
}

func TestSameNameFromTwoKeysIsTwoPeople(t *testing.T) {
	url := startRelay(t)

	bob := dial(t, url)
	introduce(t, bob, "Bob")

	// Carol называется Bob — и это её право: имя ничего не решает.
	carol := dial(t, url)
	public, private := keyFor("Carol")
	introduceWithKey(t, carol, "Bob", public, private)

	participants := presenceOf(t, bob)
	for len(participants) < 2 {
		participants = presenceOf(t, bob)
	}

	if len(participants) != 2 {
		t.Fatalf("expected two participants, got %v", participants)
	}

	for _, participant := range participants {
		if participant.Name != "Bob" {
			t.Fatalf("both are named Bob, got %v", participants)
		}
	}

	if participants[0].ID == participants[1].ID {
		t.Fatal("two different keys ended up with the same address")
	}
}

func TestAddressedEnvelopeGoesByKeyNotByName(t *testing.T) {
	url := startRelay(t)

	alice := dial(t, url)
	realBob := dial(t, url)
	impostor := dial(t, url)

	introduce(t, alice, "Alice")
	introduce(t, realBob, "Bob")

	// Самозванец берёт то же имя, но ключ у него свой.
	public, private := keyFor("Carol")
	introduceWithKey(t, impostor, "Bob", public, private)

	waitForPresence(t, alice, 3)

	send(t, alice, relay.Envelope{
		Kind:      relay.KindMessage,
		Recipient: fingerprintOf("Bob"),
		Payload:   []byte(`{"text":"для настоящего Bob"}`),
	})

	envelope := receiveKind(t, realBob, relay.KindMessage)
	if envelope.Sender != fingerprintOf("Alice") {
		t.Fatalf("expected Alice's fingerprint as sender, got %q", envelope.Sender)
	}

	// Самозванцу не досталось ничего, хотя имя у него то же самое.
	// Присутствие не в счёт: его получают все.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		_, data, err := impostor.Read(ctx)
		if err != nil {
			break
		}

		leaked, err := relay.Decode(data)
		if err == nil && leaked.Kind != relay.KindPresence {
			t.Fatalf("envelope addressed to Bob's key reached someone else with Bob's name: %s",
				leaked.Kind)
		}
	}
}

func waitForPresence(t *testing.T, conn *websocket.Conn, count int) {
	t.Helper()

	for len(presenceOf(t, conn)) < count {
	}
}

func TestAddressSurvivesReconnect(t *testing.T) {
	url := startRelay(t)

	first := dial(t, url)
	introduce(t, first, "Bob")
	receiveKind(t, first, relay.KindPresence)
	first.Close(websocket.StatusNormalClosure, "")

	// Тот же ключ возвращается под тем же именем — это обычный реконнект.
	again := dial(t, url)
	introduce(t, again, "Bob")

	participants := presenceOf(t, again)

	if len(participants) != 1 || participants[0].ID != fingerprintOf("Bob") {
		t.Fatalf("expected Bob under his own address, got %v", participants)
	}
}

func TestSilentClientIsDropped(t *testing.T) {
	url := startRelay(t)

	quiet := dial(t, url)
	receiveKind(t, quiet, relay.KindChallenge)

	// Вызов получен, ответа нет — соединение должно закрыться само.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, _, err := quiet.Read(ctx); err == nil {
		t.Fatal("client that never introduced itself stayed connected")
	}
}
