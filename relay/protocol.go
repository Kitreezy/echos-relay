package relay

import "encoding/json"

// Kind — тип конверта. Значения совпадают с RelayEnvelopeKind в iOS-клиенте.
type Kind string

const (
	// KindHello — клиент представляется. Приходит при каждом подключении:
	// после реконнекта сервер о клиенте ничего не помнит.
	KindHello Kind = "hello"

	// KindPresence — список тех, кто сейчас в комнате. Только от сервера.
	KindPresence Kind = "presence"

	KindMessage Kind = "message"
	KindTyping  Kind = "typing"
)

// Envelope — то, что ходит по сокету.
//
// Payload — это []byte, и Go кодирует его в base64, как и JSONEncoder в Swift
// для типа Data. Поэтому вложенный JSON (MessagePayload, TypingEvent, список
// имён) сервер не разбирает вообще: он его пересылает как есть.
type Envelope struct {
	Kind   Kind   `json:"kind"`
	Sender string `json:"sender"`

	// omitempty важен: в Swift payload объявлен как Data?, и для hello
	// ключа в JSON нет вовсе.
	Payload []byte `json:"payload,omitempty"`
}

func Decode(data []byte) (Envelope, error) {
	var envelope Envelope
	err := json.Unmarshal(data, &envelope)
	return envelope, err
}

func (e Envelope) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// PresenceEnvelope собирает список присутствующих.
func PresenceEnvelope(names []string) (Envelope, error) {
	payload, err := json.Marshal(names)
	if err != nil {
		return Envelope{}, err
	}

	return Envelope{Kind: KindPresence, Payload: payload}, nil
}

// StampedBy возвращает копию с подставленным отправителем.
//
// Имя берётся из того, чем клиент представился при подключении, а не из
// конверта: содержимому от клиента доверять нельзя, назваться можно кем угодно.
func (e Envelope) StampedBy(sender string) Envelope {
	e.Sender = sender
	return e
}
