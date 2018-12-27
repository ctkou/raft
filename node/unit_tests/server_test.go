package node

import (
  "context"
  "testing"
  "time"
  server "raft/node"
)

func TestMakeServer(test *testing.T) {
  udpServer := server.NewUDPServer("127.0.0.1:9999")
  if udpServer == nil {
    test.Errorf("Failed to construct udp server\n")
  }
  if udpServer.Address != "127.0.0.1:9999" {
    test.Errorf("Expected Server to be initialized to address %s, but got %s\n", "127.0.0.1:9999", udpServer.Address)
  }
}

func TestListen(test *testing.T) {
  udpServer := server.NewUDPServer("127.0.0.1:9999")
  err := udpServer.Listen()
  if err != nil {
    test.Errorf("Failed to listen on %s, with error %s\n", udpServer.Address, err)
  }
  udpServer.Close()
}

func TestListenSendAndReceive(test *testing.T) {
  ctx := context.Background()

  sender := server.NewUDPServer("127.0.0.1:9999")
  receiver := server.NewUDPServer("127.0.0.1:10000")
  receiverBuffer := make([]byte, 1024)

  receiver.Listen()

  byteSent, senderErr := sender.Send(ctx, "127.0.0.1:10000", []byte("hello world!"))
  if senderErr != nil {
    test.Errorf("Unexpected error from sender: %s\n", senderErr)
  }
  byteReceived, senderAddress, receiverErr := receiver.Receive(ctx, receiverBuffer)
  if receiverErr != nil {
    test.Errorf("Unexpected error from receiver: %s\n", receiverErr)
  }

  if byteSent != byteReceived {
    test.Errorf("Expected bytes sent equals to byte received, but %d bytes were sent and %d bytes were received\n", byteSent, byteReceived)
  }
  if byteSent == 0 || byteReceived == 0 {
    test.Errorf("Expected non-zero bytes sent and received, but %d bytes were sent and %d bytes were received\n", byteSent, byteReceived)
  }
  if sender.Address != senderAddress {
    test.Errorf("Expected sender address from receiver end to be %s, but got %s\n", sender.Address, senderAddress)
  }
  if string(receiverBuffer[:byteReceived]) != "hello world!" {
    test.Errorf("Expected 'hello world!' message received, but got %s\n", string(receiverBuffer[:byteReceived]))
  }
}

func TestReceiveTimeout(test *testing.T) {
  receiverCtx, _ := context.WithTimeout(context.Background(), 50 * time.Millisecond)
  receiver := server.NewUDPServer("127.0.0.1:10000")

  status := make(chan bool, 1)
  go func() {
    receiver.Receive(receiverCtx, make([]byte, 1024))
    status <-true
  } ()

  select {
  case <-time.After(time.Second):
    test.Errorf("Expected context to timeout and get cancelled, but it did not\n")
  case <-receiverCtx.Done():
  }
}
