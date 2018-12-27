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
  Id int
  Address string
  PeerNodes map[int]string
  CurrentTerm int
  VotedFor *int
  Log []LogEntry
  Commit int
  Applied int
  Next map[int]int
  Match map[int]int

  server Server
}

func NewNode(id int, address string, peerNodes map[int]string, currentTerm int, votedFor *int, log []LogEntry, commit int, applied int, next map[int]int, match map[int]int) *Node {
  node := new(Node)
  node.Id = id
  node.Address = address
  node.PeerNodes = peerNodes
  node.CurrentTerm = currentTerm
  node.VotedFor = votedFor
  node.Log = log
  node.Commit = commit
  node.Applied = applied
  node.Next = next
  node.Match = match
  node.server = NewUDPServer(address)
  return node
}

// // perform log replication
// func (node *Node) ReplicateLog(followerId int) {
//   // TODO
//   return
// }

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
 *  Send an AppendEntries message to the specified follower
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for the AppendEntries
 *  - followerId : follower to send the AppendEntries message to
 *
 *  return
 *  - err : error for sending the AppendEntries message
 */
func (node *Node) SendAppendEntries(ctx context.Context, followerId int) (err error) {
  payload, err := node.getAppendEntriesMessage(followerId)
  if err != nil {
    return
  }
  followerAddress := node.PeerNodes[followerId]
  _, err = node.server.Send(ctx, followerAddress, payload)
  return
}

/**
 *  Receive an AppendEntriesResult message from follower
 * 
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for the AppendEntries
 *  - buffer : buffer to write the AppendEntriesResult packet to
 *
 *  return
 *  - followerAddress : address of the follower that the AppendEntriesResult originates
 *  - err : err from receiving AppendEntriesResult
 */
func (node *Node) ReceiveAppendEntriesResult(ctx context.Context, buffer []byte) (followerAddress string, result AppendEntriesResult, err error) {
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
    followerAddress = clientAddress
    err = json.Unmarshal(buffer[:byteReceived], &result)
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
 *  Receive AppendEntries message from master
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for the AppendEntries
 *  - buffer : buffer to write the AppendEntries packet to
 */
func (node *Node) ReceiveAppendEntries(ctx context.Context, buffer []byte) (masterAddress string, appendEntries AppendEntries, err error) {
  err = node.server.Listen()
  if err != nil {
    return
  }
  done := make(chan bool, 1)
  go func() {
    byteReceived, clientAddress, serverErr := node.server.Receive(ctx, buffer)
    if serverErr != nil {
      err = serverErr
      done <- true
      return
    }
    masterAddress = clientAddress
    err = json.Unmarshal(buffer[:byteReceived], &appendEntries)
    done <-true
  } ()
  select {
  case <-ctx.Done():
    err = ctx.Err()
  case <-done:
  }
  return
}

/*
  // check if the term in AppendEntries is less than the current one
  if appendEntriesMessage.Term < node.CurrentTerm {
    return AppendEntriesResult{node.CurrentTerm, false}, nil
  }
  // check if the current log contains an entry at prevLogIndex whose term matches prevLogTerm
  prevLogIndex := appendEntriesMessage.PrevLogIndex // one-indexed
  prevLogTerm := appendEntriesMessage.PrevLogTerm
  if len(node.Log) < prevLogIndex || (len(node.Log) > 0 && node.Log[prevLogIndex - 1].Term != prevLogTerm) {
    return AppendEntriesResult{node.CurrentTerm, false}, nil
  }
  // update log accordingly
  return AppendEntriesResult{appendEntriesMessage.Term, true}, nil
*/

/**
 * return the AppendEntries message, the message include the current term of master, master id,
 * previous log index and log term of the follower, index of last log entry known to be 
 * committed, and log entries starting from previous log index + 1
 * - followerId : for extracting the previous log index and previous log term for the follower
 *
 * return
 * - payload : json representation of the AppendEntries message
 * - error : possible error when constructing the payload
 */
func (node *Node) getAppendEntriesMessage(followerId int) (payload []byte, err error) {
  prevLogIndex := node.Match[followerId] // this is one-indexed
  prevLogTerm := 0
  if prevLogIndex > 0 {
    prevLogTerm = node.Log[prevLogIndex - 1].Term
  }
  payload, err = json.Marshal(AppendEntries{node.CurrentTerm,
                                            node.Id,
                                            prevLogIndex,
                                            prevLogTerm,
                                            node.Commit,
                                            node.Log[prevLogIndex:]})
  return
}
