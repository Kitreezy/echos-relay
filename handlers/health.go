package handlers

import (
	"encoding/json"
	"net/http"
)

const Version = "1.0.0"

func Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// Ошибка записи здесь означает, что проверяющий уже отвалился, —
	// сделать с этим нечего, поэтому она отбрасывается явно.
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": Version,
	})
}
