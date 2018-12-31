package node

import (
  "context"
  "encoding/json"
)

type LogEntry struct {
  Term int
  Data []byte
}

type Node struct {
  server Server
  // Persistent
  Id int
  Address string
  PeerNodes map[int]string
  CurrentTerm int
  VotedFor *int
  Log []LogEntry
  // Volatile
  Commit int
  Applied int
}

/**
 *  Listen for incoming message on the node's address and port
 *  
 *  return
 *  - err : possible error to listen to the node's address and port
 */
func (node *Node) Listen() (err error) {
  err = node.server.Listen()
  return
}

/**
 *  Close the node's address and port from accepting further messages
 */
func (node *Node) Close() {
  node.server.Close()
  return
}

/**
 *  send a RAFT protocol message
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for sending the RAFT protocol message
 *  - address : address to send the RAFT protocol message to
 *  - data : RAFT protocol message struct (see message.go)
 */
func (node *Node) send(ctx context.Context, address string, data interface{}) (err error) {
  payload, err := json.Marshal(data)
  if err != nil {
    return
  }
  _, err = node.server.Send(ctx, address, payload)
  return
}

/**
 *  Receive a RAFT protocol message
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for receiving the RAFT protocol message
 *  - buffer : buffer to write the RAFT protocol message packet to
 *  - data : pointer to RAFT protocol message struct (see message.go)
 *
 *  return
 *  - address : address of the RAFT protocol message originates
 *  - err : error, if any, from receiving the RAFT protocol message
 */
func (node *Node) receive(ctx context.Context, buffer []byte, data interface{}) (address string, err error) {
  err = node.server.Listen()
  if err != nil {
    return
  }
  done := make(chan bool, 1)
  go func() {
    byteReceived, clientAddress, serverErr := node.server.Receive(ctx, buffer)
    if serverErr != nil {
      err = serverErr
      done <-true
      return
    }
    address = clientAddress
    err = json.Unmarshal(buffer[:byteReceived], &data)
    done <-true
  } ()
  select {
  case <-ctx.Done():
    err = ctx.Err()
  case <-done:
  }
  return
}

/**
 *  Receive an RequestVote message from a candidate
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for receiving the RequestVote message
 *  - buffer : buffer to write the RequestVote packet to
 *
 *  return
 *  - candidateAddress : address of the candidate that the RequestVote message originates
 *  - request : the RequestVote message received from the candidate
 *  - err : error, if any, from receiving the RequestVote message
 */
func (node *Node) ReceiveRequestVote(ctx context.Context, buffer []byte) (candidateAddress string, request RequestVote, err error) {
  candidateAddress, err = node.receive(ctx, buffer, &request)
  return
}

/**
 *  Send a RequestVoteResult message to the specified candidate
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for sending the RequestVoteResult message
 *  - peerId : peer Id to send the RequestVoteResult message to
 *  - voteGranted : true if vote is to be granted to the specified peer
 *
 *  return
 *  - err : error for sending the RequestVoteResult message
 */
func (node *Node) SendRequestVoteResult(ctx context.Context, peerId int, voteGranted bool) (err error) {
  return node.send(ctx, node.PeerNodes[peerId], RequestVoteResult{node.CurrentTerm, voteGranted})
}
