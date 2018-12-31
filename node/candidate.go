package node

import (
  "context"
)

type Candidate struct {
  *Node
}

func NewCandidate(id int, address string, peerNodes map[int]string, currentTerm int, votedFor *int, log []LogEntry, commit int, applied int) *Candidate {
  return &Candidate{&Node{NewUDPServer(address), id, address, peerNodes, currentTerm, votedFor, log, commit, applied}}
}

/**
 *  Send a RequestVote message to the specified peer
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for sending the RequestVote message
 *  - peerId : peer Id to send the RequestVote message to
 *
 *  return
 *  - err : error for sending the RequestVote message
 */
func (candidate *Candidate) SendRequestVote(ctx context.Context, peerId int) (err error) {
  lastLogIndex := len(candidate.Log)
  lastLogTerm := 0
  if lastLogIndex != 0 {
    lastLogTerm = candidate.Log[lastLogIndex - 1].Term
  }
  request := RequestVote{candidate.CurrentTerm, candidate.Id, lastLogIndex, lastLogTerm}
  return candidate.send(ctx, candidate.PeerNodes[peerId], request)
}

/**
 *  Receive a RequestVoteResult message from a peer
 *  - ctx : context for which caller of this method may choose to set timeout
 *          deadline for receiving a RequestVoteResult message
 *  - buffer : buffer to write the RequestVoteResult packet to
 *
 *  return
 *  - peerAddress : address of the peer that the RequestVoteResult message originates
 *  - result : the RequestVoteResult message received from the peer
 *  - err : error, if any, from receiving the RequestVoteResult message
 */
func (candidate *Candidate) ReceiveRequestVoteResult(ctx context.Context, buffer []byte) (peerAddress string, result RequestVoteResult, err error) {
  peerAddress, err = candidate.receive(ctx, buffer, &result)
  return
}
