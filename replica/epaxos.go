package replica

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epx/bloomfilter"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

const MAX_DEPTH_DEP = 10
const DS = 5
const ADAPT_TIME_SEC = 10

const MAX_BATCH = 200

const COMMIT_GRACE_PERIOD = 10 * time.Second

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
	problemInstances        []int32
	clusterTimeouts         []time.Duration
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
		problemInstances:        make([]int32, replicaCnt),
		clusterTimeouts:         make([]time.Duration, replicaCnt),
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

	cluster.SetContext(ereplica.newReplicaContext(cluster.Context()))

	return ereplica
}

func (r *epaxosReplica) Start() {
	f, err := os.OpenFile(fmt.Sprintf("logreplica_%d.log", r.id), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	log.SetOutput(f)

	log.WithFields(log.Fields{
		"addr": r.address,
	}).Info("setting up server")
	lis, err := net.Listen("tcp", r.address)
	if err != nil {
		panic(err)
	}
	var opts []grpc.ServerOption
	//	creds, err := credentials.NewServerTLSFromFile("mjolk.be.crt", "mjolk.be.key")
	//	if err != nil {
	//		panic("Failed to generate credentials %v")
	//	}
	//	opts = []grpc.ServerOption{grpc.Creds(creds)}
	grpcServer := grpc.NewServer(opts...)
	RegisterGrpcReplicaServer(grpcServer, r)
	go r.run()
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

/*func (r *epaxosReplica) ProposeAndRead(ctx context.Context, propRead *ProposalRead) (*Empty, error) {
	return &Empty{}, nil
}*/

type ctxKey int

const replicaCtx ctxKey = 0

func (r *epaxosReplica) newReplicaContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, replicaCtx, r)
}

func ReplicaFromContext(ctx context.Context) *epaxosReplica {
	if replica, ok := ctx.Value(replicaCtx).(*epaxosReplica); ok {
		return replica
	}
	return nil
}

func (r *epaxosReplica) Prepare(stream GrpcReplica_PrepareServer) error {
	for {
		p, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.cluster.Replica(p.Replica).SetIPrepareStream(stream)
		r.preparations <- p
	}
}

func (r *epaxosReplica) TryPreAccept(stream GrpcReplica_TryPreAcceptServer) error {
	log.Info("<<--trypreaccept")
	for {
		tpa, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.cluster.Replica(tpa.Leader).SetITryPreAcceptStream(stream)
		r.tryPreAcceptances <- tpa
	}
}

func (r *epaxosReplica) PreAccept(stream GrpcReplica_PreAcceptServer) error {
	log.Info("<<--preaccept")
	for {
		pa, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.cluster.Replica(pa.Leader).SetIPreAcceptStream(stream)
		r.preAcceptances <- pa
	}
}

func (r *epaxosReplica) Accept(stream GrpcReplica_AcceptServer) error {
	log.Info("<<--accept")
	for {
		a, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.cluster.Replica(a.Leader).SetIAcceptStream(stream)
		r.acceptances <- a
	}
}

func (r *epaxosReplica) Commit(stream GrpcReplica_CommitServer) error {
	log.Info("<<--COMMIT")
	for {
		c, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.commits <- c
	}
}

func (r *epaxosReplica) CommitShort(stream GrpcReplica_CommitShortServer) error {
	log.Info("<<--commmitshort")
	for {
		c, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.commitsShort <- c
	}
}

func (r *epaxosReplica) ProposeAndRead(stream GrpcReplica_ProposeAndReadServer) error {
	log.Info("<<--commmitshort")
	/*	for {
		c, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.commitsShort <- c

	}*/
	return nil
}
