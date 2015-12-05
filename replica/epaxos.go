package replica

import (
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epx/bloomfilter"
	"golang.org/x/net/context"
	"math"
	"sync"
)

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const DS = 5
const ADAPT_TIME_SEC = 10

const MAX_BATCH = 101

const COMMIT_GRACE_PERIOD = 10 * 1e9 //10 seconds

const BF_K = 4
const BF_M_N = 32.0

var bf_PT uint32

const DO_CHECKPOINTING = false
const HT_INIT_SIZE = 200000
const CHECKPOINT_PERIOD = 10000

var cpMarker []*Command
var cpcounter = 0

type InstanceSpace [][]*Instance

type Instance struct {
	commands []*Command
	ballot   int32
	status   Status
	seq      int32
	deps     []int32
	lb       *LeaderBookkeeping
	index    int
	lowlink  int
	bfilter  *bloomfilter.Bloomfilter
}

type instanceId struct {
	replica  int32
	instance int32
}

type RecoveryInstance struct {
	cmds            []*Command
	status          Status
	seq             int32
	deps            []int32
	preAcceptCount  int
	leaderResponded bool
}

type LeaderBookkeeping struct {
	clientProposals   []*Proposal
	maxRecvBallot     int32
	prepareOKs        int
	allEqual          bool
	preAcceptOKs      int
	acceptOKs         int
	nacks             int
	originalDeps      []int32
	committedDeps     []int32
	recoveryInst      *RecoveryInstance
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool
	tpaOKs            int
}

type epaxosReplica struct {
	*replica
	preparations            chan *Preparation
	preAcceptances          chan *PreAcceptance
	acceptances             chan *Acceptance
	commits                 chan *TryCommit
	commitsShort            chan *TryCommitShort
	preparationReplies      chan *PreparationReply
	preAcceptanceReplies    chan *PreAcceptanceReply
	preAcceptanceOks        chan *PreAcceptanceOk
	acceptanceReplies       chan *AcceptanceReply
	tryPreAcceptances       chan *TryPreAcceptance
	tryPreAcceptanceReplies chan *TryPreAcceptanceReply
	instanceSpace           InstanceSpace
	crtInstance             []int32
	committedUpTo           []int32
	executedUpTo            []int32
	conflicts               []map[string]int32
	maxSeqPerKey            map[string]int32
	maxSeq                  int32
	latestCPReplica         int32
	latestCPInstance        int32
	clientLock              *sync.Mutex
	recoveryInstances       chan *instanceId
	exec                    bool
}

//todo fix Key loopup hash map
func NewEpaxosReplica(id int32, address string, cluster Cluster) Replica {
	replicaCnt := cluster.Len()
	log.WithFields(log.Fields{
		"cluster": replicaCnt,
	}).Info("creating replica")
	ereplica := &epaxosReplica{
		replica:                 NewReplica(id, address, cluster),
		preparations:            make(chan *Preparation, CHAN_BUFFER_SIZE),
		preparationReplies:      make(chan *PreparationReply, CHAN_BUFFER_SIZE),
		preAcceptances:          make(chan *PreAcceptance, CHAN_BUFFER_SIZE),
		acceptances:             make(chan *Acceptance, CHAN_BUFFER_SIZE),
		commits:                 make(chan *TryCommit, CHAN_BUFFER_SIZE),
		commitsShort:            make(chan *TryCommitShort, CHAN_BUFFER_SIZE),
		preAcceptanceReplies:    make(chan *PreAcceptanceReply, CHAN_BUFFER_SIZE),
		preAcceptanceOks:        make(chan *PreAcceptanceOk, CHAN_BUFFER_SIZE),
		acceptanceReplies:       make(chan *AcceptanceReply, CHAN_BUFFER_SIZE),
		tryPreAcceptances:       make(chan *TryPreAcceptance, CHAN_BUFFER_SIZE),
		tryPreAcceptanceReplies: make(chan *TryPreAcceptanceReply, CHAN_BUFFER_SIZE),
		instanceSpace:           make([][]*Instance, replicaCnt),
		crtInstance:             make([]int32, replicaCnt),
		committedUpTo:           []int32{-1, -1, -1, -1, -1},
		executedUpTo:            make([]int32, replicaCnt),
		conflicts:               make([]map[string]int32, replicaCnt),
		maxSeqPerKey:            make(map[string]int32),
		exec:                    true,
		maxSeq:                  0,
		latestCPReplica:         0,
		latestCPInstance:        -1,
		clientLock:              new(sync.Mutex),
		recoveryInstances:       make(chan *instanceId, CHAN_BUFFER_SIZE),
	}

	for i := 0; i < replicaCnt; i++ {
		ereplica.instanceSpace[i] = make([]*Instance, 2*1024*1024)
		ereplica.crtInstance[i] = 0
		ereplica.executedUpTo[i] = -1
		ereplica.conflicts[i] = make(map[string]int32, HT_INIT_SIZE)
	}

	for bf_PT = 1; math.Pow(2, float64(bf_PT))/float64(MAX_BATCH) < BF_M_N; {
		bf_PT++
	}

	cpMarker = make([]*Command, 0)

	return ereplica
}

func (r *epaxosReplica) ReplyPropose(ctx context.Context, propReply *ProposalReply) (*Empty, error) {
	log.Info("<<--replypropose")
	return &Empty{}, nil
}

func (r *epaxosReplica) ReplyProposeTS(ctx context.Context, propReplyTS *ProposalReplyTS) (*Empty, error) {
	log.Info("<<--replyproposeTS")
	return &Empty{}, nil
}

func (r *epaxosReplica) ProposeAndRead(ctx context.Context, propRead *ProposalRead) (*Empty, error) {
	return &Empty{}, nil
}

func (r *epaxosReplica) ReplyProposeAndRead(ctx context.Context, propReadReply *ProposalReadReply) (*Empty, error) {
	return &Empty{}, nil
}

func (r *epaxosReplica) Prepare(ctx context.Context, prep *Preparation) (*Empty, error) {
	r.preparations <- prep
	return &Empty{}, nil
}

func (r *epaxosReplica) ReplyPrepare(ctx context.Context, prepReply *PreparationReply) (*Empty, error) {
	r.preparationReplies <- prepReply
	return &Empty{}, nil
}

func (r *epaxosReplica) TryPreAccept(ctx context.Context, tryPreAcceptance *TryPreAcceptance) (*Empty, error) {
	log.Info("<<--trypreaccept")
	r.tryPreAcceptances <- tryPreAcceptance
	return &Empty{}, nil
}
func (r *epaxosReplica) ReplyTryPreAccept(ctx context.Context, tryPreAcceptanceReply *TryPreAcceptanceReply) (*Empty, error) {
	log.Info("<<--trypreacceptreply")
	r.tryPreAcceptanceReplies <- tryPreAcceptanceReply
	return &Empty{}, nil
}

func (r *epaxosReplica) PreAccept(ctx context.Context, preAcceptance *PreAcceptance) (*Empty, error) {
	log.Info("<<--preaccept")
	r.preAcceptances <- preAcceptance
	return &Empty{}, nil
}

func (r *epaxosReplica) ReplyPreAccept(ctx context.Context, preAcceptanceReply *PreAcceptanceReply) (*Empty, error) {
	log.Info("<<--preacceptreply <<<-------------------------------------------------------------------------------------------------")
	r.preAcceptanceReplies <- preAcceptanceReply
	return &Empty{}, nil
}

func (r *epaxosReplica) PreAcceptOK(ctx context.Context, preAcceptanceOk *PreAcceptanceOk) (*Empty, error) {
	log.Info("<<--preacceptOK")
	r.preAcceptanceOks <- preAcceptanceOk
	return &Empty{}, nil
}

func (r *epaxosReplica) Accept(ctx context.Context, acceptance *Acceptance) (*Empty, error) {
	log.Info("<<--accept")
	r.acceptances <- acceptance
	return &Empty{}, nil
}

func (r *epaxosReplica) ReplyAccept(ctx context.Context, acceptanceReply *AcceptanceReply) (*Empty, error) {
	log.Info("<<--acceptreply")
	r.acceptanceReplies <- acceptanceReply
	return &Empty{}, nil
}

func (r *epaxosReplica) Commit(ctx context.Context, tryCommit *TryCommit) (*Empty, error) {
	log.Info("<<--COMMIT")
	r.commits <- tryCommit
	return &Empty{}, nil
}

func (r *epaxosReplica) CommitShort(ctx context.Context, tryCommitShort *TryCommitShort) (*Empty, error) {
	log.Info("<<--commmitshort")
	r.commitsShort <- tryCommitShort
	return &Empty{}, nil
}
