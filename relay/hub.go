package relay

import (
	"log"
	"slices"
	"strings"
	"sync"
)

// Client — одно подключение.
//
// Писать в вебсокет из нескольких горутин нельзя, поэтому у каждого клиента
// своя очередь и свой писатель: все, кто хочет что-то отправить, кладут в
// канал, а достаёт из него один.
type Client struct {
	ID   uint64
	Send chan []byte

	// Nonce — что этот клиент должен подписать. Своя строка на каждое
	// подключение, поэтому лежит здесь, а не в хабе.
	Nonce []byte

	mu          sync.RWMutex
	name        string
	fingerprint string
}

func (c *Client) Name() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.name
}

// Fingerprint — отпечаток ключа, которым клиент подтвердил имя.
func (c *Client) Fingerprint() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fingerprint
}

func (c *Client) setIdentity(name, fingerprint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.name = name
	c.fingerprint = fingerprint
}

// HasIntroduced — клиент прислал hello и подпись сошлась. До этого мы не
// знаем, от кого сообщения, и не обслуживаем его.
func (c *Client) HasIntroduced() bool {
	return c.Name() != ""
}

// Hub — все, кто сейчас в комнате.
//
// Состояние держится в памяти: релей ничего не хранит и не переживает
// перезапуск. Отсюда же и потолок — один инстанс. Когда понадобится второй,
// присутствие придётся выносить в Redis, иначе половина собеседников не
// увидит другую.
type Hub struct {
	mu      sync.RWMutex
	clients map[uint64]*Client
	lastID  uint64

	// sendQueueSize — насколько клиент может отстать, прежде чем его отключат.
	sendQueueSize int
}

func NewHub() *Hub {
	return &Hub{
		clients:       make(map[uint64]*Client),
		sendQueueSize: 32,
	}
}

// Register закрепляет за клиентом его личность.
//
// Реестра имён здесь нет и не нужно: адресуют по отпечатку ключа, а имя —
// подпись на экране. Два человека могут называться одинаково, и ничего от
// этого не сломается: конверт всё равно уйдёт тому, чей отпечаток указан.
//
// Подпись под nonce к этому моменту уже проверена.
func (h *Hub) Register(name string, publicKey []byte, client *Client) {
	client.setIdentity(name, Fingerprint(publicKey))
}

func (h *Hub) Add() *Client {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastID++
	client := &Client{
		ID:   h.lastID,
		Send: make(chan []byte, h.sendQueueSize),
	}
	h.clients[client.ID] = client

	return client
}

func (h *Hub) Remove(client *Client) {
	h.mu.Lock()
	_, exists := h.clients[client.ID]
	delete(h.clients, client.ID)
	h.mu.Unlock()

	if !exists {
		return
	}

	close(client.Send)

	if client.HasIntroduced() {
		log.Printf("'%s' left (%s)", client.Name(), client.Fingerprint())
		h.BroadcastPresence()
	}
}

// Presence — представившиеся, по отпечатку.
//
// Порядок стабильный намеренно: клиент сравнивает списки, чтобы не дёргать
// UI на одинаковых обновлениях, а со случайным порядком это не работает.
// Сортировка по отпечатку, а не по имени: имена могут совпадать.
func (h *Hub) Presence() []Participant {
	h.mu.RLock()
	defer h.mu.RUnlock()

	seen := make(map[string]bool, len(h.clients))
	participants := make([]Participant, 0, len(h.clients))

	for _, client := range h.clients {
		fingerprint := client.Fingerprint()

		// Один ключ может держать два соединения разом: старое ещё не убрано,
		// а клиент уже переподключился. В списке это один человек.
		if fingerprint == "" || seen[fingerprint] {
			continue
		}

		seen[fingerprint] = true
		participants = append(participants, Participant{
			ID:   fingerprint,
			Name: client.Name(),
		})
	}

	slices.SortFunc(participants, func(a, b Participant) int {
		return strings.Compare(a.ID, b.ID)
	})

	return participants
}

func (h *Hub) BroadcastPresence() {
	envelope, err := PresenceEnvelope(h.Presence())
	if err != nil {
		log.Printf("presence encode failed: %v", err)
		return
	}

	data, err := envelope.Encode()
	if err != nil {
		log.Printf("presence encode failed: %v", err)
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients {
		if client.HasIntroduced() {
			h.enqueue(client, data)
		}
	}
}

// Relay пересылает конверт всем, кроме отправителя.
func (h *Hub) Relay(data []byte, from *Client) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients {
		if client.ID != from.ID && client.HasIntroduced() {
			h.enqueue(client, data)
		}
	}
}

// RelayTo отправляет конверт одному получателю — по отпечатку его ключа.
//
// Если такого отпечатка на релее нет, конверт молча пропадает: очереди для
// отсутствующих мы не держим, а клиент всё равно сохранил росчерк у себя.
//
// Отправляется всем соединениям с этим отпечатком, а не первому попавшемуся:
// в момент переподключения их бывает два, и угадывать живое незачем.
func (h *Hub) RelayTo(data []byte, recipient string, from *Client) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	delivered := false

	for _, client := range h.clients {
		if client.ID != from.ID && client.Fingerprint() == recipient {
			h.enqueue(client, data)
			delivered = true
		}
	}

	return delivered
}

// enqueue кладёт в очередь клиента, не блокируясь.
//
// Если очередь переполнена, клиент не успевает читать — рассылка не должна
// вставать из-за одного отстающего. Такое подключение просто закрывается,
// клиент переподключится сам.
func (h *Hub) enqueue(client *Client, data []byte) {
	select {
	case client.Send <- data:
	default:
		log.Printf("'%s' is too slow, dropping connection", client.Name())
	}
}
