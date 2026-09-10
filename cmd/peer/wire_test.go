package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Приложение читает эти форматы напрямую, поэтому проверяется не «сериализуется
// ли», а «сойдётся ли с тем, что ждёт Swift».

func TestSwiftDateCountsFromTwoThousandOne(t *testing.T) {
	// 1 января 2001 года — ноль по календарю Swift.
	epoch := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

	if got := swiftDate(epoch); got != 0 {
		t.Fatalf("начало отсчёта должно быть нулём, получилось %v", got)
	}

	// А 1970-й — отрицательная величина, и это нормально.
	unixEpoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	if got := swiftDate(unixEpoch); got != -swiftEpoch {
		t.Fatalf("ожидалось %v, получилось %v", -swiftEpoch, got)
	}
}

func TestStrokeLooksTheWaySwiftExpects(t *testing.T) {
	stroke := newStroke("a1b2c3d4", []Point{{X: 0.1, Y: 0.2}})

	data, err := json.Marshal(stroke)
	if err != nil {
		t.Fatalf("не закодировался: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("не разобрался обратно: %v", err)
	}

	for _, key := range []string{"id", "author", "points", "createdAt"} {
		if _, exists := decoded[key]; !exists {
			t.Fatalf("нет поля %q: %s", key, data)
		}
	}

	// Swift кодирует UUID заглавными буквами, и его декодер строчные
	// принимает — но пусть будет как у него.
	if id, _ := decoded["id"].(string); id != strings.ToUpper(id) {
		t.Fatalf("идентификатор должен быть в верхнем регистре: %q", id)
	}

	// Дата — число, а не строка: JSONEncoder по умолчанию пишет её так.
	if _, isNumber := decoded["createdAt"].(float64); !isNumber {
		t.Fatalf("createdAt должно быть числом: %s", data)
	}
}

func TestIdentityIsStableForTheSameName(t *testing.T) {
	first := identityFor("Борис")
	again := identityFor("Борис")

	if first.fingerprint() != again.fingerprint() {
		t.Fatal("один и тот же собеседник должен возвращаться с тем же адресом")
	}

	if identityFor("Вера").fingerprint() == first.fingerprint() {
		t.Fatal("разные имена не должны давать один адрес")
	}
}

// Ради -fresh: тёзка отличается от знакомого именно ключом.
func TestFreshIdentityDiffersEveryTime(t *testing.T) {
	if freshIdentity().fingerprint() == freshIdentity().fingerprint() {
		t.Fatal("новый ключ должен быть новым")
	}
}

// Росчерк из одной точки приложение выбрасывает как случайное касание,
// и такая заглушка рисовала бы в пустоту.
func TestDiagonalIsAStrokeNotATap(t *testing.T) {
	points := diagonal()

	if len(points) < 2 {
		t.Fatalf("одна точка — это касание, а не росчерк: %v", points)
	}

	for _, point := range points {
		if point.X < 0 || point.X > 1 || point.Y < 0 || point.Y > 1 {
			t.Fatalf("точка вне стены: %v", point)
		}
	}

	if points[0] == points[len(points)-1] {
		t.Fatal("росчерк должен куда-то вести")
	}
}
