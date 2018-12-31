package node

import (
  "context"
)

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
 *          deadline for sending the AppendEntries message
 *  - followerId : follower Id to send the AppendEntries message to
 *
 *  return
 *  - err : error for sending the AppendEntries message
 */
func (leader *Leader) SendAppendEntries(ctx context.Context, followerId int) (err error) {
  prevLogIndex := leader.Match[followerId] // this is one-indexed
  prevLogTerm := 0
  if prevLogIndex > 0 {
    prevLogTerm = leader.Log[prevLogIndex - 1].Term
  }
  appendEntries := AppendEntries{leader.CurrentTerm, leader.Id, prevLogIndex, prevLogTerm, leader.Commit, leader.Log[prevLogIndex:]}
  return leader.send(ctx, leader.PeerNodes[followerId], appendEntries)
}

/**
 *  Receive an AppendEntriesResult message from a follower
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for receiving an AppendEntriesResult message
 *  - buffer : buffer to write the AppendEntriesResult packet to
 *
 *  return
 *  - followerAddress : address of the follower that the AppendEntriesResult message originates
 *  - result : the AppendEntriesResult message received from the follower
 *  - err : error, if any, from receiving the AppendEntriesResult message
 */
func (leader *Leader) ReceiveAppendEntriesResult(ctx context.Context, buffer []byte) (followerAddress string, result AppendEntriesResult, err error) {
  followerAddress, err = leader.receive(ctx, buffer, &result)
  return
}
