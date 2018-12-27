package node

import (
  "context"
  // "fmt"
  "net"
)

func NewUDPServer(address string) *UDPServer {
  server := new(UDPServer)
  server.Address = address
  return server
}

type UDPServer struct {
  Connection *net.UDPConn
  Address string
}

func (server *UDPServer) Listen() (err error) {
  if server.Connection == nil {
    server.Connection = new(net.UDPConn)
    resolvedUDPAddress, udpErr := net.ResolveUDPAddr("udp", server.Address)
    if udpErr != nil {
      return udpErr
    }
    server.Connection, err = net.ListenUDP("udp", resolvedUDPAddress)
  }
  return
}

func (server *UDPServer) Close() {
  if server.Connection != nil {
    (*server.Connection).Close()
    server.Connection = nil
  }
}

/**
 *  Receive on specified server address and port, write packet to buffer when a packet arrives
 *  - ctx : context for which the Receive call may be cancelled by the caller
 *  - buffer : the buffer to write the packet to
 *  
 *  return
 *  - byteRead : the number of bytes read from the incoming packet
 *  - clientAddress : address of incoming packet
 *  - err : nil if a packet is successfully received
 */
func (server *UDPServer) Receive(ctx context.Context, buffer []byte) (byteRead int64, clientAddress string, err error) {
  if server.Connection == nil {
    err = server.Listen()
    if err != nil {
      return
    }
    defer server.Close()
  }

  status := make(chan error, 1)

  go func() {
    read, address, readErr := server.Connection.ReadFrom(buffer)
    if readErr != nil {
      status <-readErr
      return
    }
    // fmt.Printf("%d byte(s) is received from %s\n", read, address)
    clientAddress = address.String()
    byteRead = int64(read)
    status <-nil
  } ()

  select {
    case <-ctx.Done():
      err = ctx.Err()
    case err = <-status:
  }
  return
}

/**
 *  Send a UDP packet from the specified server address to the specified client address
 *  - ctx : context for which the Send call may be cancelled by the caller
 *  - clientAddress : the client address to receive the packet
 *  - data : the content of the out going packet
 *
 *  return
 *  - byteWritten : the number of bytes being written out
 *  - err : nil if a packet is successfully sent
 */
func (server *UDPServer) Send(ctx context.Context, clientAddress string, data []byte) (byteWritten int64, err error) {
  // establish udp connection if none is established
  if server.Connection == nil {
    err = server.Listen()
    if err != nil {
      return
    }
    defer server.Close()
  }

  // resolve client addresses
  resolvedClientAddress, err := net.ResolveUDPAddr("udp", clientAddress)
  if err != nil {
    return
  }

  // initiate go routine to send message
  status := make(chan error, 1)
  go func() {
    written, writeErr := server.Connection.WriteToUDP(data, resolvedClientAddress)
    if writeErr != nil {
      status <- writeErr
      return
    }
    // fmt.Printf("%d byte(s) is sent to %s\n", written, clientAddress)
    byteWritten = int64(written)
    status <-nil
  } ()

  select {
    case <-ctx.Done():
      err = ctx.Err()
    case <-status:
  }

  return
}
