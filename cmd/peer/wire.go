package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"echos-relay/relay"
)

// Форматы полезной нагрузки повторяют модели приложения — не потому что так
// красивее, а потому что иначе оно просто не прочитает. Swift кодирует Data в
// base64, а Go делает то же самое с []byte, поэтому конверт сходится сам; а
// вот внутренности приходится держать в руках.

// swiftEpoch — начало отсчёта дат в Swift: 1 января 2001 года.
//
// `JSONEncoder` по умолчанию пишет `Date` как секунды от этого момента, а не
// от 1970-го. Ошибиться здесь легко, и цена ошибки — росчерк, датированный
// тридцатью годами раньше, который уедет в конец стены.
const swiftEpoch = 978307200

func swiftDate(t time.Time) float64 {
	return float64(t.Unix()-swiftEpoch) + float64(t.Nanosecond())/1e9
}

// Message — то же, что MessagePayload в приложении.
type Message struct {
	ID         string  `json:"id"`
	Text       string  `json:"text"`
	SenderName string  `json:"senderName"`
	Timestamp  float64 `json:"timestamp"` // здесь от 1970-го: поле объявлено Double
}

// Typing — то же, что TypingEvent.
type Typing struct {
	Type      string  `json:"type"` // start | stop
	PeerName  string  `json:"peerName"`
	Timestamp float64 `json:"timestamp"`
}

// Point — доля ширины и высоты стены, от нуля до единицы.
type Point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Stroke — росчерк. `id` в верхнем регистре: Swift кодирует UUID именно так.
type Stroke struct {
	ID        string  `json:"id"`
	Author    string  `json:"author"`
	Points    []Point `json:"points"`
	CreatedAt float64 `json:"createdAt"`
}

func newStroke(author string, points []Point) Stroke {
	return Stroke{
		ID:        strings.ToUpper(newUUID()),
		Author:    author,
		Points:    points,
		CreatedAt: swiftDate(time.Now()),
	}
}

// diagonal — точки росчерка через всю стену.
//
// Точек больше одной намеренно: приложение считает росчерк из одной точки
// случайным касанием и выбрасывает его, как выбросило бы задевший экран палец.
func diagonal() []Point {
	const count = 12

	points := make([]Point, 0, count)
	for i := range count {
		fraction := 0.1 + 0.8*float64(i)/(count-1)
		points = append(points, Point{X: fraction, Y: fraction})
	}

	return points
}

// newUUID — четвёртая версия, без внешних зависимостей.
func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// identity — ключ, которым заглушка подписывает рукопожатие.
type identity struct {
	public  ed25519.PublicKey
	private ed25519.PrivateKey
}

// identityFor — устойчивый ключ, выведенный из имени.
//
// Устойчивый намеренно: приложение узнаёт собеседника по отпечатку, и если бы
// ключ менялся при каждом запуске, «Борис» после перезапуска выглядел бы
// новым человеком. А чтобы изобразить как раз самозванца — есть -fresh.
func identityFor(name string) identity {
	seed := sha256.Sum256([]byte("echos-peer:" + name))
	private := ed25519.NewKeyFromSeed(seed[:])

	return identity{public: private.Public().(ed25519.PublicKey), private: private}
}

func freshIdentity() identity {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	return identity{public: public, private: private}
}

func (i identity) fingerprint() string {
	sum := sha256.Sum256(i.public)
	return hex.EncodeToString(sum[:8])
}

func (i identity) helloPayload(challenge []byte) ([]byte, error) {
	return json.Marshal(relay.HelloPayload{
		PublicKey: i.public,
		Signature: ed25519.Sign(i.private, challenge),
	})
}
