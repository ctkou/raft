package node

import (
  "context"
)

type Follower struct {
  *Node
}

func NewFollower(id int, address string, peerNodes map[int]string, currentTerm int, votedFor *int, log []LogEntry, commit int, applied int) *Follower {
  return &Follower{&Node{NewUDPServer(address), id, address, peerNodes, currentTerm, votedFor, log, commit, applied}}
}

/**
 *  Send an AppendEntriesResult message to the specified master
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for sending the AppendEntriesResult message
 *  - masterId : master Id to send the AppendEntriesResult message to
 *  - success : true if previous AppendEntries message was processed successfully
 *
 *  return
 *  - err : error for sending the AppendEntriesResult message
 */
func (follower *Follower) SendAppendEntriesResult(ctx context.Context, masterId int, success bool) (err error) {
  return follower.send(ctx, follower.PeerNodes[masterId], AppendEntriesResult{follower.CurrentTerm, success})
}

/**
 *  Receive an AppendEntries message from the master
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for receiving an AppendEntries message
 *  - buffer : buffer to write the AppendEntries packet to
 *
 *  return
 *  - masterAddress : address of the master that the AppendEntries message originates
 *  - appendEntries: the AppendEntries message received from the master
 *  - err : error, if any, from receiving the AppendEntries message
 */
func (follower *Follower) ReceiveAppendEntries(ctx context.Context, buffer []byte) (masterAddress string, appendEntries AppendEntries, err error) {
  masterAddress, err = follower.receive(ctx, buffer, &appendEntries)
  return
}
