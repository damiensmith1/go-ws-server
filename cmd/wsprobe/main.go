// Command wsprobe is a small interactive client for the WebSocket server,
// useful for smoke tests and ad-hoc poking. It connects, then bidirectionally
// proxies stdin (one JSON frame per line, sent as text) to the socket and
// every incoming frame to stdout.
//
// Examples
//
//	wsprobe -url ws://localhost:8080/ws -userKey alice
//	wsprobe -url ws://localhost:8080/ws -token "$JWT" -keepAlive
//
// Then type one JSON message per line:
//
//	{"type":"subscribe","topic":"chat"}
//	{"type":"publish","topic":"chat","data":{"hi":1}}
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
)

func main() {
	target := flag.String("url", "ws://localhost:8080/ws", "WebSocket URL")
	userKey := flag.String("userKey", "", "userKey (insecure mode)")
	token := flag.String("token", "", "JWT (auth mode)")
	keepAlive := flag.Bool("keepAlive", false, "request server-driven ping/pong")
	flag.Parse()

	u, err := url.Parse(*target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad -url: %v\n", err)
		os.Exit(2)
	}
	q := u.Query()
	if *userKey != "" {
		q.Set("userKey", *userKey)
	}
	if *token != "" {
		q.Set("token", *token)
	}
	if *keepAlive {
		q.Set("keepAlive", "true")
	}
	u.RawQuery = q.Encode()

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Fprintf(os.Stderr, "connected to %s — type one JSON message per line\n", u.String())

	// Reader goroutine: print every inbound frame.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Fprintf(os.Stderr, "read: %v\n", err)
				}
				return
			}
			fmt.Println(string(msg))
		}
	}()

	// Stdin → socket.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigs
		_ = c.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
		c.Close()
	}()

	scan := bufio.NewScanner(os.Stdin)
	scan.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scan.Scan() {
		line := scan.Bytes()
		if len(line) == 0 {
			continue
		}
		if err := c.WriteMessage(websocket.TextMessage, line); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			return
		}
	}
	if err := scan.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin: %v\n", err)
	}

	_ = c.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	<-done
}
