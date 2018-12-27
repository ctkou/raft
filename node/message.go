package node

type AppendEntries struct {
  Term int
  LeaderId int
  PrevLogIndex int
  PrevLogTerm int
  LastCommit int
  Entries []LogEntry
}

type AppendEntriesResult struct {
  Term int
  Success bool
}

type RequestVote struct {
  Term int
  CandidateId int
  LastLogIndex int
  LastLogTerm int
}

type RequestVoteResult struct {
  Term int
  VoteGranted bool
}
