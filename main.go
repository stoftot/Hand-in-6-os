package main

import (
	"math/rand"
	"sync"
	"time"
)

// Define server states
const (
	Follower = iota
	Candidate
	Leader
)

// Message types for communication
type MessageType int

const (
	// RPCs
	AppendEntriesRPC MessageType = iota
	RequestVoteRPC
	// RPC responses
	AppendEntriesResponse
	RequestVoteResponse
)

// LogEntry represents a command entry in the log
type LogEntry struct {
	Term    int
	Command interface{}
}

// Message is used to simulate RPC calls between servers
type Message struct {
	From     int
	To       int
	Type     MessageType
	Term     int
	Success  bool
	LogIndex int
	Data     interface{}
}

// Raft represents a single Raft server
type Raft struct {
	mu          sync.Mutex    // Mutex to protect shared access to this server's state
	id          int           // Server ID
	peers       []int         // IDs of all peers
	state       int           // Current state (Follower, Candidate, Leader)
	term        int           // Current term
	votedFor    int           // CandidateId that received vote in current term (or -1 if none)
	log         []LogEntry    // Log entries
	commitIdx   int           // Index of highest log entry known to be committed
	lastApplied int           // Index of highest log entry applied to state machine
	nextIndex   []int         // For each server, index of the next log entry to send to that server
	matchIndex  []int         // For each server, index of highest log entry known to be replicated on server
	msgChan     chan Message  // Channel to receive messages (RPCs)
	timeout     time.Duration // Election timeout duration
	heartbeat   time.Duration // Heartbeat interval
}

// NewRaft creates a new Raft server
func NewRaft(id int, peers []int) *Raft {
	r := &Raft{
		id:          id,
		peers:       peers,
		state:       Follower,
		term:        0,
		votedFor:    -1,
		log:         make([]LogEntry, 0),
		commitIdx:   0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		msgChan:     make(chan Message, 100),
		timeout:     time.Duration(rand.Intn(150)+150) * time.Millisecond, // Randomized timeout between 150-300ms
		heartbeat:   50 * time.Millisecond,                                // Heartbeat every 50ms
	}
	return r
}

// Run starts the main loop for the Raft server
func (r *Raft) Run() {
	// Start a goroutine to process incoming messages
	go r.processMessages()

	// Main loop
	for {
		switch r.state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower handles the follower state
func (r *Raft) runFollower() {
	// Set a timer for election timeout
	timeout := time.NewTimer(r.timeout)

	for r.state == Follower {
		select {
		case <-timeout.C:
			// Timeout elapsed, convert to candidate
			r.state = Candidate
		case msg := <-r.msgChan:
			switch msg.Type {
			case AppendEntriesRPC:
				r.handleAppendEntries(msg)
				// Reset the election timeout on receiving a valid AppendEntries RPC
				timeout.Reset(r.timeout)
			case RequestVoteRPC:
				r.handleRequestVote(msg)
			}
		}
	}
}

// runCandidate handles the candidate state
func (r *Raft) runCandidate() {
	// Increment current term and vote for self
	r.mu.Lock()
	r.term++
	r.votedFor = r.id
	r.mu.Unlock()

	// Send RequestVote RPCs to all other servers
	votes := 1
	var mu sync.Mutex
	finished := make(chan bool)

	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		go func(peer int) {
			// Send RequestVote RPC
			msg := Message{
				From: r.id,
				To:   peer,
				Type: RequestVoteRPC,
				Term: r.term,
			}
			send(msg)

			// Wait for response
			response := <-r.msgChan
			if response.Type == RequestVoteResponse && response.Term == r.term {
				mu.Lock()
				if response.Success {
					votes++
				}
				mu.Unlock()
			}
			finished <- true
		}(peer)
	}

	// Set a timer for election timeout
	timeout := time.NewTimer(r.timeout)

	// Wait for responses or timeout
	for i := 0; i < len(r.peers)-1; i++ {
		select {
		case <-finished:
			// Check if received majority votes
			mu.Lock()
			if votes > len(r.peers)/2 {
				// Become leader
				r.mu.Lock()
				r.state = Leader
				r.mu.Unlock()
				mu.Unlock()
				return
			}
			mu.Unlock()
		case <-timeout.C:
			// Election timeout, start new election
			r.mu.Lock()
			r.state = Candidate
			r.mu.Unlock()
			return
		}
	}

	// If not enough votes, remain as follower
	r.mu.Lock()
	r.state = Follower
	r.mu.Unlock()
}

// runLeader handles the leader state
func (r *Raft) runLeader() {
	// Initialize nextIndex and matchIndex for each follower
	for i := range r.peers {
		r.nextIndex[i] = len(r.log)
		r.matchIndex[i] = 0
	}

	// Start sending heartbeats to followers
	ticker := time.NewTicker(r.heartbeat)
	defer ticker.Stop()

	for r.state == Leader {
		select {
		case <-ticker.C:
			// Send AppendEntries RPCs (heartbeats) to all followers
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				}
				go func(peer int) {
					msg := Message{
						From: r.id,
						To:   peer,
						Type: AppendEntriesRPC,
						Term: r.term,
					}
					send(msg)
				}(peer)
			}
		case msg := <-r.msgChan:
			switch msg.Type {
			case AppendEntriesResponse:
				// Handle AppendEntries response
				if msg.Success {
					// Update matchIndex and nextIndex
					r.matchIndex[msg.From] = msg.LogIndex
					r.nextIndex[msg.From] = msg.LogIndex + 1
				} else {
					// Decrement nextIndex and retry
					r.nextIndex[msg.From]--
				}
			case RequestVoteRPC:
				// Handle RequestVote RPC (another server starting election)
				r.handleRequestVote(msg)
			}
		}
	}
}

// handleAppendEntries handles the AppendEntries RPC
func (r *Raft) handleAppendEntries(msg Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if msg.Term < r.term {
		// Reply false if term is less than current term
		response := Message{
			From:    r.id,
			To:      msg.From,
			Type:    AppendEntriesResponse,
			Term:    r.term,
			Success: false,
		}
		send(response)
		return
	}

	// If term >= current term, become follower
	r.state = Follower
	r.term = msg.Term
	r.votedFor = -1

	// TODO: Simplify log consistency check for brevity

	// Reply true
	response := Message{
		From:    r.id,
		To:      msg.From,
		Type:    AppendEntriesResponse,
		Term:    r.term,
		Success: true,
	}
	send(response)
}

// handleRequestVote handles the RequestVote RPC
func (r *Raft) handleRequestVote(msg Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize response
	response := Message{
		From:    r.id,
		To:      msg.From,
		Type:    RequestVoteResponse,
		Term:    r.term,
		Success: false,
	}

	if msg.Term < r.term {
		// Reply false if term is less than current term
		send(response)
		return
	}

	// If term >= current term, update current term and become follower
	if msg.Term > r.term {
		r.term = msg.Term
		r.votedFor = -1
		r.state = Follower
	}

	// If haven't voted or voted for candidate, grant vote
	if r.votedFor == -1 || r.votedFor == msg.From {
		r.votedFor = msg.From
		response.Success = true
	}
	send(response)
}

// processMessages handles incoming messages
func (r *Raft) processMessages() {
	for {
		select {
		case msg := <-network[r.id]:
			r.msgChan <- msg
		}
	}
}

// send simulates sending a message over the network
func send(msg Message) {
	// Simulate network delay
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	network[msg.To] <- msg
}

// network simulates the message passing between servers
var network []chan Message

func main() {
	// Initialize servers
	numServers := 5
	network = make([]chan Message, numServers)
	peers := make([]int, numServers)
	for i := 0; i < numServers; i++ {
		network[i] = make(chan Message, 100)
		peers[i] = i
	}

	// Create and start Raft servers
	for i := 0; i < numServers; i++ {
		raft := NewRaft(i, peers)
		go raft.Run()
	}

	// Run indefinitely
	select {}
}
