package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"os"
	"path/filepath"
	"reflect"
)

var (
	storagePath, listenAddr string
)

func TestAppendGet(t *testing.T) {
	dir, err := ioutil.TempDir(".", "e2e_test_")
	if err != nil {
		t.Fatal("unexpected err: %v", err)
	}
	defer os.RemoveAll(dir)
	var tests = []struct {
		description                         string
		retention, gcInterval, pushInterval time.Duration
	}{
		{
			description:  "default values",
			retention:    24 * time.Hour,
			gcInterval:   10 * time.Minute,
			pushInterval: 5 * time.Second,
		},
	}
	storagePath = filepath.Join(dir, "messages.db")
	listenAddr = ":9099"

	for _, test := range tests {
		t.Logf("running text with context: %#v", test)
		go func() {
			t.Logf("starting server")
			if err := runService(storagePath, listenAddr, test.retention, test.gcInterval, test.pushInterval); err != nil {
				t.Fatalf("server encountered unexpected error: %v", err)
			}
		}()

		if err := waitServerStart(); err != nil {
			t.Fatalf("server encountered unexpected error: %v", err)
		}

		genID, err := getGenerationID()
		if err != nil {
			t.Fatalf("failed to retrieve generation ID from server: %v", err)
		}

		topics := []string{
			"topic0",
			"topic1",
			"topic2",
			"topic3",
		}
		items := []map[string]interface{}{
			{"A": "Hi", "B": 0.0},
			{"A": "Hello", "B": 1.0},
			{"A": "Bonjour", "B": 2.0},
			{"A": "Hola", "B": 3.0},
			{"A": "Shalon", "B": 4.0},
		}
		for _, topic := range topics {
			idx := 1
			for _, item := range items {
				if err := doAppend(item, topic); err != nil {
					t.Fatalf("failed to perform append: %v", err)
				}
				fromIndex := fmt.Sprintf("%v", idx)
				//return 1 object at a time
				msgs, err := doGet(topic, genID, fromIndex)
				if err != nil {
					t.Fatalf("failed to get messages from server: %v", err)
				}
				if msgs.GenerationID != genID {
					t.Fatalf("server did not return expected generation ID: %s != %s", msgs.GenerationID, genID)
				}
				if len(msgs.Messages) != 1 {
					t.Fatalf("server did not return expected number of objects: %v != 1", len(msgs.Messages))
				}
				msg := msgs.Messages[0]
				retItem, ok := msg.Data.(map[string]interface{})
				if !ok {
					t.Fatalf("type of message did not match expected: %v != map[string]interface{}", reflect.TypeOf(retItem))
				}
				if !reflect.DeepEqual(retItem, item) {
					t.Fatalf("returned item did not match expected: %v != %v", retItem, item)
				}
				idx++
			}
		}
	}
}

func waitServerStart() error {
	done := make(chan struct{})
	go func() {
		for {
			_, err := getGenerationID()
			if err == nil {
				close(done)
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()
	select {
	case <-done:
		return nil
	case <-time.After(time.Second * 5):
		return fmt.Errorf("server didn't start for 5 seconds")
	}
}

func getGenerationID() (string, error) {
	msgs, err := doGet("_invalid_topic_", "", "")
	if err != nil {
		return "", err
	}
	return msgs.GenerationID, nil
}

func doAppend(v interface{}, topic string) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	resp, err := doHttpRequest("POST", "/topics/"+topic, nil, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("invalid response to HTTP POST: status %s, body: %s", resp.Status, data)
	}
	return nil
}

func doGet(topic, genID, fromIdx string) (*MessagesResponse, error) {
	query := make(url.Values)
	query.Set("generationID", genID)
	query.Set("fromIndex", fromIdx)
	resp, err := doHttpRequest("GET", "/topics/"+topic, query, nil)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("invalid response to HTTP POST: status %s, body: %s", resp.Status, data)
	}
	var msgs MessagesResponse
	if err := json.Unmarshal(data, &msgs); err != nil {
		return nil, err
	}
	return &msgs, err
}

func initiateWatch(topic, genID, fromIdx string) (<-chan *MessagesResponse, <-chan error, error) {
	query := make(url.Values)
	query.Set("generationID", genID)
	query.Set("fromIndex", fromIdx)
	u := url.URL{
		Scheme:   "ws",
		Host:     "localhost:9090",
		Path:     "/topics" + topic + "/watch",
		RawQuery: query.Encode(),
	}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	msgsChan := make(chan *MessagesResponse)
	errChan := make(chan error)
	go func() {
		for {
			var msgs MessagesResponse
			if err := conn.ReadJSON(&msgs); err != nil {
				errChan <- err
				return
			}
			msgsChan <- &msgs
		}
	}()
	return msgsChan, errChan, nil
}

func doHttpRequest(method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	u := url.URL{
		Scheme: "http",
		Host:   "localhost" + listenAddr,
		Path:   path,
	}
	if query != nil && len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}
