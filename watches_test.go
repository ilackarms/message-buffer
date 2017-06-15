package main

import (
	"testing"

	"fmt"
	"github.com/gorilla/websocket"
	"net/http/httptest"
	"net/url"
	"time"
	"github.com/gorilla/mux"
)

var subject = "WatchManager"

type testNotificationStore struct {
	notifications []Notification
}

func (s *testNotificationStore) append(v interface{}) error {
	s.notifications = append(s.notifications, Notification{
		Index:     uint64(len(s.notifications)),
		Timestamp: time.Now(),
		Data:      v,
	})
	return nil
}

func (s *testNotificationStore) get(generationID string, fromIndex uint64) (*NotificationsResponse, error) {
	i := int(fromIndex)
	return &NotificationsResponse{
		GenerationID:  generationID,
		Notifications: s.notifications[i:],
	}, nil
}

func (s *testNotificationStore) close() error {
	return nil
}

func TestWatch(t *testing.T) {
	var tests = []struct {
		Context      string
		Expectation  string
		MessageCount int
		MessageDelay time.Duration
		PollInterval time.Duration
	}{
		{
			Context:      "New messages created every 1s",
			Expectation:  "send messages to client every pollInterval",
			MessageCount: 10,
			MessageDelay: time.Second,
			PollInterval: time.Second * 2,
		},
	}

	for _, test := range tests {
		runWatchTest(t, test)
	}
}

func runWatchTest(t *testing.T, test struct {
	Context      string
	Expectation  string
	MessageCount int
	MessageDelay time.Duration
	PollInterval time.Duration
}) {
	t.Logf("When %s, %s should %s", test.Context, subject, test.Expectation)

	store := &testNotificationStore{}
	dialer := websocket.DefaultDialer
	r := mux.NewRouter()
	watchManager := NewWatchManager(store, test.PollInterval)

	r.HandleFunc("/{topic}/watch", watchManager.HandleWatchRequest)
	server := httptest.NewServer(r)
	defer server.Close()
	u, _ := url.Parse(server.URL)
	u.Scheme = "ws"
	u.Path = "/mytopic/watch"

	conn, resp, err := dialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("unexpected error connecting: %v\nresponse: %#v", err, resp)
	}
	messageChan := make(chan *NotificationsResponse)
	go func() {
		defer close(messageChan)
		for {
			var notificationsResponse *NotificationsResponse
			if err := conn.ReadJSON(&notificationsResponse); err != nil {
				return
			}
			messageChan <- notificationsResponse
		}
	}()

	submittedMessages := []string{}
	go func() {
		for i := 0; i < test.MessageCount; i++ {
			item := fmt.Sprintf("message #%v", i)
			store.append(item)
			submittedMessages = append(submittedMessages, item)
			time.Sleep(test.MessageDelay)
		}
	}()

	receivedItems := 0
	select {
	case <-time.After((test.PollInterval + test.MessageDelay) * time.Duration(test.MessageCount)):
		t.Fatal("timed out waiting for messages to be received")
	case notificationsResponse := <-messageChan:
		for _, notification := range notificationsResponse.Notifications {
			item := notification.Data.(string)
			if item != submittedMessages[receivedItems] {
				t.Fatalf("expected received message %s to equal sent message %s", item, submittedMessages[receivedItems])
			}
			receivedItems++
			if receivedItems == test.MessageCount {
				return
			}
		}
	}
}
