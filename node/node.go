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

func (node *Node) Close() {
  node.server.Close()
  return
}

type Leader struct {
  Next map[int]int
  Match map[int]int
  *Node
}

func NewLeader(id int, address string, peerNodes map[int]string,
               currentTerm int, votedFor *int, log []LogEntry, commit int,
               applied int, next map[int]int, match map[int]int) *Leader {
  return &Leader{next,
                 match,
                 &Node{NewUDPServer(address), id, address, peerNodes,
                       currentTerm, votedFor, log, commit, applied}}
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
func (leader *Leader) SendAppendEntries(ctx context.Context, followerId int) (err error) {
  payload, err := leader.getAppendEntriesMessage(followerId)
  if err != nil {
    return
  }
  followerAddress := leader.PeerNodes[followerId]
  _, err = leader.server.Send(ctx, followerAddress, payload)
  return
}

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
func (leader *Leader) getAppendEntriesMessage(followerId int) (payload []byte, err error) {
  prevLogIndex := leader.Match[followerId] // this is one-indexed
  prevLogTerm := 0
  if prevLogIndex > 0 {
    prevLogTerm = leader.Log[prevLogIndex - 1].Term
  }
  payload, err = json.Marshal(AppendEntries{leader.CurrentTerm,
                                            leader.Id,
                                            prevLogIndex,
                                            prevLogTerm,
                                            leader.Commit,
                                            leader.Log[prevLogIndex:]})
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
func (leader *Leader) ReceiveAppendEntriesResult(ctx context.Context, buffer []byte) (followerAddress string, result AppendEntriesResult, err error) {
  err = leader.server.Listen()
  if err != nil {
    return
  }
  done := make(chan bool, 1)
  go func() {
    byteReceived, clientAddress, serverErr := leader.server.Receive(ctx, buffer)
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

type Follower struct {
  *Node
}

func NewFollower(id int, address string, peerNodes map[int]string, currentTerm int, votedFor *int, log []LogEntry, commit int, applied int) *Follower {
  return &Follower{&Node{NewUDPServer(address), id, address, peerNodes, currentTerm, votedFor, log, commit, applied}}
}

/**
 *  Receive AppendEntries message from master
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for the AppendEntries
 *  - buffer : buffer to write the AppendEntries packet to
 */
func (follower *Follower) ReceiveAppendEntries(ctx context.Context, buffer []byte) (masterAddress string, appendEntries AppendEntries, err error) {
  err = follower.server.Listen()
  if err != nil {
    return
  }
  done := make(chan bool, 1)
  go func() {
    byteReceived, clientAddress, serverErr := follower.server.Receive(ctx, buffer)
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

/**
 *  Send an AppendEntriesResult message to the master
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for the AppendEntriesResult
 *  - masterAddress : master address to send the AppendEntriesResult message to
 *  - success : true if the original AppendEntries was accepted
 *
 *  return
 *  - err : error for sending the AppendEntriesResult message
 */
func (follower *Follower) SendAppendEntriesResult(ctx context.Context, masterAddress string, success bool) (err error) {
  payload, err := follower.getAppendEntriesResultMessage(success)
  if err != nil {
    return
  }
  _, err = follower.server.Send(ctx, masterAddress, payload)
  return
}

/**
 * return the AppendEntriesResult message, the message include the current term
 * of master, master id, previous log index and log term of the follower, index
 * of last log entry known to be committed, and log entries starting from 
 * previous log index + 1
 * - followerId : for extracting the previous log index and previous log term 
 *                for the follower
 *
 * return
 * - payload : json representation of the AppendEntries message
 * - error : possible error when constructing the payload
 */
func (follower *Follower) getAppendEntriesResultMessage(success bool) (payload []byte, err error) {
  payload, err = json.Marshal(AppendEntriesResult{follower.CurrentTerm, success})
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
