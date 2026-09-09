package relay

import (
	"log"
	"slices"
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

	mu   sync.RWMutex
	name string
}

func (c *Client) Name() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.name
}

func (c *Client) SetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.name = name
}

// HasIntroduced — клиент прислал hello. До этого мы не знаем, от кого
// сообщения, и не обслуживаем его.
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
		log.Printf("'%s' left", client.Name())
		h.BroadcastPresence()
	}
}

// Presence — имена представившихся, по алфавиту.
//
// Порядок стабильный намеренно: клиент сравнивает списки, чтобы не дёргать
// UI на одинаковых обновлениях, а со случайным порядком это не работает.
func (h *Hub) Presence() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.clients))
	for _, client := range h.clients {
		if name := client.Name(); name != "" {
			names = append(names, name)
		}
	}

	slices.Sort(names)
	return names
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

// RelayTo отправляет конверт одному получателю.
//
// Если такого имени на релее нет, конверт молча пропадает: очереди для
// отсутствующих мы не держим, а клиент всё равно сохранил росчерк у себя.
func (h *Hub) RelayTo(data []byte, recipient string, from *Client) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients {
		if client.ID != from.ID && client.Name() == recipient {
			h.enqueue(client, data)
			return true
		}
	}

	return false
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
