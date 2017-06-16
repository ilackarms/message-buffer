package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
	"net/url"
	"github.com/gorilla/websocket"
)

var (
	storagePath, listenAddr string
)

func TestE2E(t *testing.T) {
	dir, err := ioutil.TempDir(".", "e2e_test_")
	if err != nil {
		t.Fatal("unexpected err: %v", err)
	}
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
	storagePath = dir
	listenAddr = ":9099"

	for _, test := range tests {
		t.Logf("running text with context: %#v", test)
		go func() {
			t.Logf("starting server")
			if err := runService(storagePath, listenAddr, test.retention, test.gcInterval, test.pushInterval); err != nil {
				t.Fatal("server encountered unexpected error: %v", err)
			}
		}()
	}
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
		Scheme: "ws",
		Host: "localhost:9090",
		Path: "/topics"+topic+"/watch",
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
		Host: "localhost"+listenAddr,
		Path: path,
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
