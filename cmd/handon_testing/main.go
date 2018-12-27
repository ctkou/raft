package main

import (
  "fmt"
  "context"
  "strings"
  server "raft/node"
)

func main() {
  ctx := context.Background()
  buffer := make([]byte, 1024)

  // listen to port 9999
  done := make(chan bool, 1)
  go func() {
    server.Receive(ctx, "127.0.0.1:9999", buffer)
    done <-true
  } ()

  // send a message from port 9998 to port 9999
  go server.Send(ctx, "127.0.0.1:9998", "127.0.0.1:9999", strings.NewReader("Hello"))

  switch {
  case <-done:
    fmt.Println(string(buffer[:5]))
    return
  }
}
