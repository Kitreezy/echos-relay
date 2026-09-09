package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port string

	// PingInterval — как часто слать клиенту ping. Нужен не только для
	// keep-alive: половина хостингов и NAT-роутеров рвут молчащее соединение,
	// а прокси Render закрывает его после минуты тишины.
	PingInterval time.Duration

	// PongTimeout — сколько ждать ответа, прежде чем считать клиента мёртвым.
	PongTimeout time.Duration

	// MaxMessageBytes — потолок на входящий кадр.
	MaxMessageBytes int64

	// Room — общая комната. Пока одна на всех; когда понадобятся отдельные
	// чаты, имя переедет в путь запроса.
	Room string
}

func Load() Config {
	return Config{
		Port:            getEnv("PORT", "8080"),
		PingInterval:    time.Duration(getEnvInt("PING_INTERVAL_SEC", 20)) * time.Second,
		PongTimeout:     time.Duration(getEnvInt("PONG_TIMEOUT_SEC", 10)) * time.Second,
		MaxMessageBytes: int64(getEnvInt("MAX_MESSAGE_KB", 64)) * 1024,
		Room:            getEnv("ROOM", "default"),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
