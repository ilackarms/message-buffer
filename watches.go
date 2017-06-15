package main

import (
	"net/http"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"time"
	"strconv"
	"github.com/gorilla/mux"
)

type WatchManager struct {
	upgrader *websocket.Upgrader
	store notificationStore
	pushInterval time.Duration
}

func NewWatchManager(store notificationStore, pushInterval time.Duration) *WatchManager {
	return &WatchManager{
		upgrader: &websocket.Upgrader{},
		store: store,
		pushInterval: pushInterval,
	}
}

func (wm *WatchManager) HandleWatchRequest(w http.ResponseWriter, r *http.Request){
	topic, ok := mux.Vars(r)["topic"]
	if !ok {
		log.Printf("error: topic not provided")
		http.Error(w, "must provide topic", http.StatusBadRequest)
		return
	}
	log.Printf("TODO: use topics: %v", topic)

	conn, err := wm.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade HTTP connection: %v", err)
		http.Error(w, fmt.Sprintf("failed to upgrade HTTP connection: %v", err), http.StatusInternalServerError)
		return
	}


	genID := r.URL.Query().Get("generationID")
	fromIdx := r.URL.Query().Get("fromIndex")

	if fromIdx == "" {
		fromIdx = "0"
	}

	idx, err := strconv.ParseUint(fromIdx, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid 'fromIndex': %v", err), http.StatusBadRequest)
		return
	}

	go wm.manageWatch(conn, genID, idx)
}

func (wm *WatchManager) manageWatch(conn *websocket.Conn, genID string, idx uint64) {
	log.Printf("connection accepted from %v", conn.RemoteAddr())
	defer closeConn(conn)
	for {
		notificationResponse, err := wm.store.get(genID, idx)
		if err != nil {
			handleError(err, conn)
			return
		}
		if err := conn.WriteJSON(notificationResponse); err != nil {
			handleError(err, conn)
			return
		}
		time.Sleep(wm.pushInterval)
	}
}

func closeConn(conn *websocket.Conn) {
	log.Printf("terminating connection to %v", conn.RemoteAddr())
	if err := conn.Close(); err != nil {
		log.Printf("[WARNING] error closing connection: %v", err)
	}
}

func handleError(err error, conn *websocket.Conn) error {
	log.Printf("closing connection due to error: %v", err)
	return conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error()))
}
