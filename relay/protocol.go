package relay

import "encoding/json"

// Kind — тип конверта. Значения совпадают с RelayEnvelopeKind в iOS-клиенте.
type Kind string

const (
	// KindChallenge — сервер → клиент: случайная строка, которую нужно
	// подписать. Уходит первой, сразу после рукопожатия.
	KindChallenge Kind = "challenge"

	// KindHello — клиент представляется. Приходит при каждом подключении:
	// после реконнекта сервер о клиенте ничего не помнит.
	//
	// В payload лежит HelloPayload — открытый ключ и подпись под nonce.
	// Одного имени мало: назваться можно кем угодно.
	KindHello Kind = "hello"

	// KindPresence — список тех, кто сейчас в комнате. Только от сервера.
	KindPresence Kind = "presence"

	KindMessage Kind = "message"
	KindTyping  Kind = "typing"

	// KindStroke — росчерк на стене. Сервер, как и с сообщениями, содержимое
	// не разбирает: точки лежат в payload и его не касаются.
	KindStroke Kind = "stroke"
)

// Participant — один человек в комнате.
//
// Имя и адрес разделены намеренно. Имя человек выбирает сам, оно может
// повторяться и меняться; адрес — отпечаток ключа, он уникален и подделать
// его нельзя, не имея закрытой части.
type Participant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Envelope — то, что ходит по сокету.
//
// Payload — это []byte, и Go кодирует его в base64, как и JSONEncoder в Swift
// для типа Data. Поэтому вложенный JSON (MessagePayload, TypingEvent, список
// имён) сервер не разбирает вообще: он его пересылает как есть.
type Envelope struct {
	Kind Kind `json:"kind"`

	// Sender — отпечаток ключа отправителя, а не имя. Сервер проставляет его
	// сам, из того ключа, которым клиент подтвердил подключение.
	//
	// Имя для этого не годится: назваться Bob может кто угодно, и входящее
	// от самозванца легло бы в переписку с настоящим Bob.
	Sender string `json:"sender"`

	// Recipient — отпечаток получателя. Пусто — всем, кроме отправителя.
	//
	// Нужен для стены: росчерк адресован конкретному человеку, и рассылать
	// его всем неправильно — при трёх участниках рисунок для одного увидят
	// остальные.
	Recipient string `json:"recipient,omitempty"`

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

// HelloPayload — то, чем клиент доказывает право на имя.
//
// Ключ приходит целиком, а не отпечатком: проверить подпись по отпечатку
// нельзя, а хранить чужие ключи между запусками релею негде.
type HelloPayload struct {
	PublicKey []byte `json:"publicKey"`
	Signature []byte `json:"signature"`
}

func DecodeHello(payload []byte) (HelloPayload, error) {
	var hello HelloPayload
	err := json.Unmarshal(payload, &hello)
	return hello, err
}

// ChallengeEnvelope кладёт nonce прямо в payload, без вложенного JSON:
// это просто набор байтов, разбирать в нём нечего.
func ChallengeEnvelope(nonce []byte) Envelope {
	return Envelope{Kind: KindChallenge, Payload: nonce}
}

// PresenceEnvelope собирает список присутствующих.
func PresenceEnvelope(participants []Participant) (Envelope, error) {
	payload, err := json.Marshal(participants)
	if err != nil {
		return Envelope{}, err
	}

	return Envelope{Kind: KindPresence, Payload: payload}, nil
}

// StampedBy возвращает копию с подставленным отправителем.
//
// Отпечаток берётся из ключа, которым клиент подтвердил подключение, а не из
// конверта: содержимому от клиента доверять нельзя.
func (e Envelope) StampedBy(fingerprint string) Envelope {
	e.Sender = fingerprint
	return e
}
