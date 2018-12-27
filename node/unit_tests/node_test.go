package node

import (
  "context"
  "testing"
  node "raft/node"
)

func TestAppendEntries(test *testing.T) {
  // create a context
  ctx := context.Background()
  // make a leader node
  leader := node.NewNode(1,                                                                    // node id
                         "127.0.0.1:10000",                                                    // node address
                         map[int]string{2 : "127.0.0.1:10001"},                                // peer nodes
                         1,                                                                    // current term
                         nil,                                                                  // voted for
                         []node.LogEntry{node.LogEntry{Term: 1, Data: []byte("Hello World!")}},// log
                         0,                                                                    // commited
                         0,                                                                    // applied
                         map[int]int{2 : 1},                                                   // next
                         map[int]int{2 : 0})                                                   // match
  // listener to our leader
  follower := node.NewNode(2,                                                                  // node id
                           "127.0.0.1:10001",                                                  // node address
                           map[int]string{1 : "127.0.0.1:10000"},                              // peer nodes
                           1,                                                                  // current term
                           nil,                                                                // voted for
                           []node.LogEntry{},                                                  // log
                           0,                                                                  // commited
                           0,                                                                  // applied
                           map[int]int{},                                                      // next
                           map[int]int{})                                                      // match
  follower.Listen()
  // leader send the message
  senderErr := leader.SendAppendEntries(ctx, 2)
  if senderErr != nil {
    test.Errorf("Failed to send AppendEntries message to follower due to %s\n", senderErr)
  }
  // receive
  buffer := make([]byte, 1024)
  masterAddress, actual, receiverErr := follower.ReceiveAppendEntries(ctx, buffer)
  // verify
  if receiverErr != nil {
    test.Errorf("Failed to receive AppendEntries message from leader due to %s\n", receiverErr)
  }
  if masterAddress != "127.0.0.1:10000" {
    test.Errorf("Expected master address of 127.0.0.1:10000, but got %s\n", masterAddress)
  }
  verifyAppendEntries(test, node.AppendEntries{1, 1, 0, 0, 0, []node.LogEntry{node.LogEntry{1, []byte("Hello World!")}}}, actual)
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
