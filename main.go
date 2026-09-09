// echos-relay — вебсокет-релей для echos.
//
// Принял соединение, запомнил имя, разослал остальным. Ничего не хранит:
// ни истории, ни аккаунтов, ни базы. Всё состояние — список тех, кто сейчас
// на связи, и живёт оно в памяти процесса.
package main

import (
	"log"
	"net/http"

	"echos-relay/config"
	"echos-relay/handlers"
	"echos-relay/relay"
)

func main() {
	cfg := config.Load()
	hub := relay.NewHub()

	ws := &handlers.WS{Hub: hub, Cfg: cfg}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handlers.Health)
	mux.HandleFunc("GET /ws", ws.Serve)

	log.Printf("echos-relay listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
		log.Fatal(err)
	}
}
