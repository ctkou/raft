package node

import (
  "context"
  "testing"
  "time"
  "strconv"
  node "raft/node"
)

func TestSendAndReceiveAppendEntries(test *testing.T) {
  // create a context that timeouts in 1 second
  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()
  // make a leader node and a follower node
  factory := makeNodeFactory()
  leader := factory.makeLeader()
  follower := factory.makeFollower()
  leader.Log = append(leader.Log, node.LogEntry{Term: 1, Data: []byte("Hello World!")})
  follower.Listen()
  defer leader.Close()
  defer follower.Close()
  // leader sends the message
  senderErr := leader.SendAppendEntries(ctx, 2)
  if senderErr != nil {
    test.Errorf("Failed to send AppendEntries message to follower due to %s\n", senderErr)
  }
  // follower receives
  buffer := make([]byte, 1024)
  leaderAddress, actual, receiverErr := follower.ReceiveAppendEntries(ctx, buffer)
  if receiverErr != nil {
    test.Errorf("Failed to receive AppendEntries message from leader due to %s\n", receiverErr)
  }
  verifyAppendEntries(test, node.AppendEntries{1, 1, 0, 0, 0, []node.LogEntry{node.LogEntry{1, []byte("Hello World!")}}}, actual)
  if leaderAddress != leader.Address {
    test.Errorf("Expected leader address of %s, but got %s\n", leader.Address, leaderAddress)
  }
}

func TestSendAndReceiveAppendEntriesResult(test *testing.T) {
  // create a context that timeouts in 1 second
  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()
  // make a leader node and a follower node
  factory := makeNodeFactory()
  leader := factory.makeLeader()
  follower := factory.makeFollower()
  leader.Listen()
  defer leader.Close()
  defer follower.Close()
  // follower sends a AppendEntriesResult message indicating successful processing of AppendEntries
  senderErr := follower.SendAppendEntriesResult(ctx, leader.Id, true)
  if senderErr != nil {
    test.Errorf("Failed to send AppendEntriesResult message to leader due to %d\n", senderErr)
  }
  // leader receives
  buffer := make([]byte, 1024)
  followerAddress, actual, receiverErr := leader.ReceiveAppendEntriesResult(ctx, buffer)
  if receiverErr != nil {
    test.Errorf("Failed to receive AppendEntries message from leader due to %s\n", receiverErr)
  }
  if actual.Term != follower.CurrentTerm {
    test.Errorf("Expected AppendEntriesResult with Term %d, but got %d\n", follower.CurrentTerm, actual.Term)
  }
  if !actual.Success {
    test.Errorf("Expected AppendEntriesResult indicating successful processing, but got unsuccessfult result\n")
  }
  if followerAddress != follower.Address {
    test.Errorf("Expected follower address of %s, but got %s\n", follower.Address, followerAddress)
  }
}

func TestSendAndReceiveRequestVote(test *testing.T) {
  // create a context that timeouts in 1 second
  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()
  // make a leader node and a follower node
  factory := makeNodeFactory()
  candidate := factory.makeCandidate()
  candidate.Log = append(candidate.Log, node.LogEntry{Term: candidate.CurrentTerm, Data: []byte("Hello World!")})
  peer := factory.makeFollower()
  peer.Listen()
  defer candidate.Close()
  defer peer.Close()
  // sends a RequestVote message to peer
  senderErr := candidate.SendRequestVote(ctx, peer.Id)
  if senderErr != nil {
    test.Errorf("Failed to send RequestVote message to peer due to %s\n", senderErr)
  }
  // peer receives
  buffer := make([]byte, 1024)
  candidateAddress, request, receiverErr := peer.ReceiveRequestVote(ctx, buffer)
  if receiverErr != nil {
    test.Errorf("Failed to receive RequestVote message from peer due to %s\n", receiverErr)
  }
  if request.Term != candidate.CurrentTerm {
    test.Errorf("Expected RequestVote message with Term %d, but got %d\n", candidate.CurrentTerm, request.Term)
  }
  if request.CandidateId != candidate.Id {
    test.Errorf("Expected RequestVote message with CandidateId %d, but got %d\n", candidate.Id, request.CandidateId)
  }
  if request.LastLogIndex != len(candidate.Log) {
    test.Errorf("Expected RequestVote message with LastLogIndex %d, but got %d\n", len(candidate.Log), request.LastLogIndex)
  }
  if request.LastLogTerm != candidate.Log[len(candidate.Log) - 1].Term {
    test.Errorf("Expected RequestVote message with LastLogTerm %d, but got %d\n", candidate.Log[len(candidate.Log) - 1].Term, request.LastLogTerm)
  }
  if candidateAddress != candidate.Address {
    test.Errorf("Expected candidate address of %s, but got %s\n", candidate.Address, candidateAddress)
  }
}

func TestSendAndReceiveRequestVoteResult(test *testing.T) {
  // create a context that timeouts in 1 second
  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()
  // make a leader node and a follower node
  factory := makeNodeFactory()
  peer := factory.makeFollower()
  candidate := factory.makeCandidate()
  candidate.Listen()
  defer peer.Close()
  defer candidate.Close()
  // sends a RequestVote message to peer
  senderErr := peer.SendRequestVoteResult(ctx, candidate.Id, true)
  if senderErr != nil {
    test.Errorf("Failed to send RequestVoteResult message to candidate due to %s\n", senderErr)
  }
  // candidate receives
  buffer := make([]byte, 1024)
  peerAddress, result, receiverErr := candidate.ReceiveRequestVoteResult(ctx, buffer)
  if receiverErr != nil {
    test.Errorf("Failed to receive RequestVoteResult message from peer due to %s\n", receiverErr)
  }
  if result.Term != peer.CurrentTerm {
    test.Errorf("Expected RequestVoteResult message with Term %d, but got %d\n", peer.CurrentTerm, result.Term)
  }
  if !result.VoteGranted {
    test.Errorf("Expected RequestVoteResult message with VoteGranted == true, but got false\n")
  }
  if peerAddress != peer.Address {
    test.Errorf("Expected peer address of %s, but got %s\n", peer.Address, peerAddress)
  }
}

type NodeFactory struct {
  nextId int
  nextPort int
  leaders []*node.Leader
  followers []*node.Follower
  candidates []*node.Candidate
}

func makeNodeFactory() (factory *NodeFactory) {
  return &NodeFactory{1, 10000, []*node.Leader{}, []*node.Follower{}, []*node.Candidate{}}
}

func (factory *NodeFactory) makeLeader() (leader *node.Leader) {
  // leader id
  id := factory.getNextId()
  // port
  port := factory.getNextPort()
  // peer nodes
  peers := factory.getAllPeers()
  leader = node.NewLeader(id, "127.0.0.1:" + port, peers, 1, nil, []node.LogEntry{}, 0, 0, map[int]int{}, map[int]int{})
  factory.updateAllPeers(id, "127.0.0.1:" + port)
  factory.leaders = append(factory.leaders, leader)
  return
}

func (factory *NodeFactory) makeFollower() (follower *node.Follower) {
  // leader id
  id := factory.getNextId()
  // port
  port := factory.getNextPort()
  // peer nodes
  peers := factory.getAllPeers()
  follower = node.NewFollower(id, "127.0.0.1:" + port, peers, 1, nil, []node.LogEntry{}, 0, 0)
  factory.updateAllPeers(id, "127.0.0.1:" + port)
  factory.followers = append(factory.followers, follower)
  return
}

func (factory *NodeFactory) makeCandidate() (candidate *node.Candidate) {
  // leader id
  id := factory.getNextId()
  // port
  port := factory.getNextPort()
  // peer nodes
  peers := factory.getAllPeers()
  candidate = node.NewCandidate(id,  "127.0.0.1:" + port, peers, 1, nil, []node.LogEntry{}, 0, 0)
  factory.updateAllPeers(id, "127.0.0.1:" + port)
  factory.candidates = append(factory.candidates, candidate)
  return
}

func (factory *NodeFactory) getNextId() (nextId int) {
  nextId = factory.nextId
  factory.nextId += 1
  return
}

func (factory *NodeFactory) getNextPort() (nextPort string) {
  nextPort = strconv.Itoa(factory.nextPort)
  factory.nextPort += 1
  return
}

func (factory *NodeFactory) getAllPeers() (peers map[int]string) {
  peers = map[int]string{}
  for i := 0; i < len(factory.leaders); i++ {
    peers[factory.leaders[i].Id] = factory.leaders[i].Address
  }
  for j := 0; j < len(factory.followers); j++ {
    peers[factory.followers[j].Id] = factory.followers[j].Address
  }
  for k := 0; k < len(factory.candidates); k++ {
    peers[factory.candidates[k].Id] = factory.candidates[k].Address
  }
  return
}

func (factory *NodeFactory) updateAllPeers(newId int, newAddress string) {
  for i := 0; i < len(factory.leaders); i++ {
    factory.leaders[i].PeerNodes[newId] = newAddress
  }
  for j := 0; j < len(factory.followers); j++ {
    factory.followers[j].PeerNodes[newId] = newAddress
  }
  for k := 0; k < len(factory.candidates); k++ {
    factory.candidates[k].PeerNodes[newId] = newAddress
  }
  return
}

func verifyAppendEntries(test *testing.T, expected, actual node.AppendEntries) {
  if actual.Term != expected.Term {
    test.Errorf("Expected AppendEntries with Term %d, but got %d\n", expected.Term, actual.Term)
  }
  if actual.LeaderId != expected.LeaderId {
    test.Errorf("Expected AppendEntries with LeaderId %d, but got %d\n", expected.LeaderId, actual.LeaderId)
  }
  if actual.PrevLogIndex != expected.PrevLogIndex {
    test.Errorf("Expected AppendEntries with previous log index %d, but got %d\n", expected.PrevLogIndex, actual.PrevLogIndex)
  }
  if actual.PrevLogTerm != expected.PrevLogTerm {
    test.Errorf("Expected AppendEntries with previous log term %d, but got %d\n", expected.PrevLogTerm, actual.PrevLogTerm)
  }
  if actual.LastCommit != expected.LastCommit {
    test.Errorf("Expected AppendEntries with last commit index %d, but got %d\n", expected.LastCommit, actual.LastCommit)
  }
  if len(actual.Entries) != len(expected.Entries) {
    test.Errorf("Expected AppendEntries with %d entry of log, but got %d\n", len(expected.Entries), len(actual.Entries))
  } else {
    for i := 0; i < len(actual.Entries); i++ {
      if actual.Entries[i].Term != expected.Entries[i].Term {
        test.Errorf("Expected AppendEntries[%d] with Term %d, but got %d\n", i, expected.Entries[i].Term, actual.Entries[i].Term)
      }
      if string(actual.Entries[i].Data) != string(expected.Entries[i].Data) {
        test.Errorf("Expected AppendEntries[%d] with data '%s', but got '%s'\n", i, expected.Entries[i].Data, actual.Entries[i].Data)
      }
    }
  }
}

