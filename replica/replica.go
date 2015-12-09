package replica

import (
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epx/rdtsc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	//"google.golang.org/grpc/credentials"
)

const CHAN_BUFFER_SIZE = 200000

type Replica interface {
	Id() int32
	Addr() string
	Close()
	IsThrifty() bool
	DoesExec() bool
	DoesDReply() bool
	IsBeacon() bool
	IsDurable() bool
	Store() Store
	SetStore(Store)
	Start()
	Ping(context.Context, *Beacon) (*Empty, error)
	ReplyPing(context.Context, *BeaconReply) (*Empty, error)
	Propose(context.Context, *ClientProposal) (*Empty, error)
	ProposeStream(GrpcReplica_ProposeStreamServer) error
	ReplyPropose(context.Context, *ProposalReply) (*Empty, error)
	ReplyProposeTS(context.Context, *ProposalReplyTS) (*Empty, error)
	ProposeAndRead(context.Context, *ProposalRead) (*Empty, error)
	ReplyProposeAndRead(context.Context, *ProposalReadReply) (*Empty, error)
	Prepare(context.Context, *Preparation) (*Empty, error)
	ReplyPrepare(context.Context, *PreparationReply) (*Empty, error)
	TryPreAccept(context.Context, *TryPreAcceptance) (*Empty, error)
	ReplyTryPreAccept(context.Context, *TryPreAcceptanceReply) (*Empty, error)
	PreAccept(context.Context, *PreAcceptance) (*Empty, error)
	ReplyPreAccept(context.Context, *PreAcceptanceReply) (*Empty, error)
	PreAcceptOK(context.Context, *PreAcceptanceOk) (*Empty, error)
	Accept(context.Context, *Acceptance) (*Empty, error)
	ReplyAccept(context.Context, *AcceptanceReply) (*Empty, error)
	Commit(context.Context, *TryCommit) (*Empty, error)
	CommitShort(context.Context, *TryCommitShort) (*Empty, error)
}

type RemoteReplica interface {
	Id() int32
	Addr() string
	Close()
	IsThrifty() bool
	DoesExec() bool
	DoesDReply() bool
	IsBeacon() bool
	IsDurable() bool
	SetClient(GrpcReplicaClient)
	Ping(context.Context, *Beacon, ...grpc.CallOption) (*Empty, error)
	ReplyPing(context.Context, *BeaconReply, ...grpc.CallOption) (*Empty, error)
	Propose(context.Context, *ClientProposal, ...grpc.CallOption) (*Empty, error)
	ProposeStream(context.Context, ...grpc.CallOption) (GrpcReplica_ProposeStreamClient, error)
	ReplyPropose(context.Context, *ProposalReply, ...grpc.CallOption) (*Empty, error)
	ReplyProposeTS(context.Context, *ProposalReplyTS, ...grpc.CallOption) (*Empty, error)
	ProposeAndRead(context.Context, *ProposalRead, ...grpc.CallOption) (*Empty, error)
	ReplyProposeAndRead(context.Context, *ProposalReadReply, ...grpc.CallOption) (*Empty, error)
	Prepare(context.Context, *Preparation, ...grpc.CallOption) (*Empty, error)
	ReplyPrepare(context.Context, *PreparationReply, ...grpc.CallOption) (*Empty, error)
	TryPreAccept(context.Context, *TryPreAcceptance, ...grpc.CallOption) (*Empty, error)
	ReplyTryPreAccept(context.Context, *TryPreAcceptanceReply, ...grpc.CallOption) (*Empty, error)
	PreAccept(context.Context, *PreAcceptance, ...grpc.CallOption) (*Empty, error)
	ReplyPreAccept(context.Context, *PreAcceptanceReply, ...grpc.CallOption) (*Empty, error)
	PreAcceptOK(context.Context, *PreAcceptanceOk, ...grpc.CallOption) (*Empty, error)
	Accept(context.Context, *Acceptance, ...grpc.CallOption) (*Empty, error)
	ReplyAccept(context.Context, *AcceptanceReply, ...grpc.CallOption) (*Empty, error)
	Commit(context.Context, *TryCommit, ...grpc.CallOption) (*Empty, error)
	CommitShort(context.Context, *TryCommitShort, ...grpc.CallOption) (*Empty, error)
	ProposeStreamServer() GrpcReplica_ProposeStreamClient
}

//float64func NewReplica(id int32)

type replica struct {
	id        int32
	address   string
	cluster   Cluster
	store     Store
	beacons   chan *Beacon
	proposals chan *Proposal
	shutdown  chan bool
	thrifty   bool
	exec      bool
	dreply    bool
	beacon    bool
	durable   bool
	ewma      []float64
}

type Proposal struct {
	*ClientProposal
	client GrpcReplica_ProposeStreamServer
}

type remoteReplica struct {
	id            int32
	address       string
	shutdown      chan bool
	thrifty       bool
	exec          bool
	dreply        bool
	beacon        bool
	durable       bool
	proposeStream GrpcReplica_ProposeStreamClient
	GrpcReplicaClient
}

func NewReplica(id int32, address string, cluster Cluster) *replica {
	replicaCnt := cluster.Len()
	replica := &replica{
		id:        id,
		address:   address,
		cluster:   cluster,
		beacons:   make(chan *Beacon, CHAN_BUFFER_SIZE),
		proposals: make(chan *Proposal, CHAN_BUFFER_SIZE),
		beacon:    true,
		exec:      false,
		thrifty:   false,
		durable:   false,
		dreply:    false,
		ewma:      make([]float64, replicaCnt),
	}
	for i := 0; i < replicaCnt; i++ {
		replica.ewma[i] = 0.0
	}
	return replica
}

func NewRemoteReplica(id int32, address string) RemoteReplica {
	return &remoteReplica{id: id, address: address}
}

func (r *remoteReplica) ProposeStreamServer() GrpcReplica_ProposeStreamClient {
	var err error
	if r.proposeStream == nil {
		r.proposeStream, err = r.ProposeStream(context.Background())
		if err != nil {
			log.Fatal("Could not create poposal stream")
		}
	}
	return r.proposeStream
}

func (r *remoteReplica) Id() int32 {
	return r.id
}

func (r *remoteReplica) Addr() string {
	return r.address
}

func (r *remoteReplica) Close() {
	r.shutdown <- true
}

func (r *remoteReplica) IsThrifty() bool {
	return r.thrifty
}

func (r *remoteReplica) DoesExec() bool {
	return r.exec
}

func (r *remoteReplica) DoesDReply() bool {
	return r.dreply
}

func (r *remoteReplica) IsBeacon() bool {
	return r.beacon
}

func (r *remoteReplica) IsDurable() bool {
	return r.durable
}

func setLog(id int32) {
}

func (r *replica) Store() Store {
	return r.store
}

func (r *replica) SetStore(store Store) {
	r.store = store
}

func (r *replica) Cluster() Cluster {
	return r.cluster
}

func (r *replica) Id() int32 {
	return r.id
}

func (r *replica) Addr() string {
	return r.address
}

func (r *replica) Beacons() chan *Beacon {
	return r.beacons
}

func (r *replica) Proposals() chan *Proposal {
	return r.proposals
}

func (r *replica) Close() {
	r.shutdown <- true
}

func (r *replica) IsThrifty() bool {
	return r.thrifty
}

func (r *replica) DoesExec() bool {
	return r.exec
}

func (r *replica) DoesDReply() bool {
	return r.dreply
}

func (r *replica) IsBeacon() bool {
	return r.beacon
}

func (r *replica) IsDurable() bool {
	return r.durable
}

func (r *replica) Log() {}

func (r *replica) Ewma() []float64 {
	return r.ewma
}

func (r *replica) Ping(ctx context.Context, beacon *Beacon) (*Empty, error) {
	r.beacons <- beacon
	return &Empty{}, nil
}

func (r *replica) ReplyPing(ctx context.Context, beaconReply *BeaconReply) (*Empty, error) {
	ewma := r.ewma[beaconReply.Replica]
	r.ewma[beaconReply.Replica] = 0.99*ewma + 0.01*float64(rdtsc.Cputicks()-uint64(beaconReply.Timestamp))
	return &Empty{}, nil
}

func (r *replica) Propose(ctx context.Context, prop *ClientProposal) (*Empty, error) {
	log.WithFields(log.Fields{
		"proposal received": prop,
	}).Info("received rpoposal")
	r.proposals <- &Proposal{prop, nil}
	return &Empty{}, nil
}

func (r *remoteReplica) SetClient(client GrpcReplicaClient) {
	r.GrpcReplicaClient = client
}

func (r *replica) ProposeStream(stream GrpcReplica_ProposeStreamServer) error {
	for {
		cp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.proposals <- &Proposal{cp, stream}
	}
}
