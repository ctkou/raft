package node

import (
  "context"
  "testing"
  "time"
  node "raft/node"
)

func TestSendAndReceiveAppendEntries(test *testing.T) {
  // create a context that timeouts in 1 second
  ctx, cancel := context.WithTimeout(context.Background(), time.Second)
  defer cancel()
  // make a leader node and a follower node
  leader, follower := getLeaderAndFollower()
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
  leader, follower := getLeaderAndFollower()
  leader.Listen()
  defer leader.Close()
  defer follower.Close()
  // follower sends a AppendEntriesResult message indicating successful processing of AppendEntries
  senderErr := follower.SendAppendEntriesResult(ctx, leader.Address, true)
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

func getLeaderAndFollower() (leader *node.Leader, follower *node.Follower) {
  // make a leader node
  leader = node.NewLeader(1,                                        // node id
                          "127.0.0.1:10000",                        // node address
                          map[int]string{2 : "127.0.0.1:10001"},    // peer nodes
                          1,                                        // current term
                          nil,                                      // voted for
                          []node.LogEntry{},                        // log
                          0,                                        // commited
                          0,                                        // applied
                          map[int]int{2 : 1},                       // next
                          map[int]int{2 : 0})                       // match
  // listener to our leader
  follower = node.NewFollower(2,                                      // node id
                              "127.0.0.1:10001",                      // node address
                              map[int]string{1 : "127.0.0.1:10000"},  // peer nodes
                              1,                                      // current term
                              nil,                                    // voted for
                              []node.LogEntry{},                      // log
                              0,                                      // commited
                              0)                                      // applied
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

