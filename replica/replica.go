package replica

import (
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epaxos_grpc/rdtsc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/credentials"
	"net"
)

const CHAN_BUFFER_SIZE = 200000

const NIL = ""

type Store interface {
	Lock()
	UnLock()
	Get(string) string
	Put(string, string)
}

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
	Start()
	Ping(context.Context, *Beacon) (*Empty, error)
	ReplyPing(context.Context, *BeaconReply) (*Empty, error)
	Propose(context.Context, *Proposal) (*Empty, error)
	ReplyPropose(context.Context, *ProposalReply) (*Empty, error)
	ReplyProposeTS(context.Context, *ProposalReplyTS) (*Empty, error)
	Read(context.Context, *Key) (*Empty, error)
	ReplyRead(context.Context, *Value) (*Empty, error)
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
	Ping(ctx context.Context, in *Beacon, opts ...grpc.CallOption) (*Empty, error)
	ReplyPing(ctx context.Context, in *BeaconReply, opts ...grpc.CallOption) (*Empty, error)
	Propose(ctx context.Context, in *Proposal, opts ...grpc.CallOption) (*Empty, error)
	ReplyPropose(ctx context.Context, in *ProposalReply, opts ...grpc.CallOption) (*Empty, error)
	ReplyProposeTS(ctx context.Context, in *ProposalReplyTS, opts ...grpc.CallOption) (*Empty, error)
	Read(ctx context.Context, in *Key, opts ...grpc.CallOption) (*Empty, error)
	ReplyRead(ctx context.Context, in *Value, opts ...grpc.CallOption) (*Empty, error)
	ProposeAndRead(ctx context.Context, in *ProposalRead, opts ...grpc.CallOption) (*Empty, error)
	ReplyProposeAndRead(ctx context.Context, in *ProposalReadReply, opts ...grpc.CallOption) (*Empty, error)
	Prepare(ctx context.Context, in *Preparation, opts ...grpc.CallOption) (*Empty, error)
	ReplyPrepare(ctx context.Context, in *PreparationReply, opts ...grpc.CallOption) (*Empty, error)
	TryPreAccept(ctx context.Context, in *TryPreAcceptance, opts ...grpc.CallOption) (*Empty, error)
	ReplyTryPreAccept(ctx context.Context, in *TryPreAcceptanceReply, opts ...grpc.CallOption) (*Empty, error)
	PreAccept(ctx context.Context, in *PreAcceptance, opts ...grpc.CallOption) (*Empty, error)
	ReplyPreAccept(ctx context.Context, in *PreAcceptanceReply, opts ...grpc.CallOption) (*Empty, error)
	PreAcceptOK(ctx context.Context, in *PreAcceptanceOk, opts ...grpc.CallOption) (*Empty, error)
	Accept(ctx context.Context, in *Acceptance, opts ...grpc.CallOption) (*Empty, error)
	ReplyAccept(ctx context.Context, in *AcceptanceReply, opts ...grpc.CallOption) (*Empty, error)
	Commit(ctx context.Context, in *TryCommit, opts ...grpc.CallOption) (*Empty, error)
	CommitShort(ctx context.Context, in *TryCommitShort, opts ...grpc.CallOption) (*Empty, error)
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

type remoteReplica struct {
	id       int32
	address  string
	shutdown chan bool
	thrifty  bool
	exec     bool
	dreply   bool
	beacon   bool
	durable  bool
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

func (r *remoteReplica) SetClient(client GrpcReplicaClient) {
	r.GrpcReplicaClient = client
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

func (r *epaxosReplica) Start() {
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

func (r *replica) Store() Store {
	return nil
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

func (r *replica) State() Store {
	return r.store
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
	log.WithFields(log.Fields{
		"ts": beacon.Timestamp,
	}).Info("received ping")
	r.beacons <- beacon
	return &Empty{}, nil
}

func (r *replica) ReplyPing(ctx context.Context, beaconReply *BeaconReply) (*Empty, error) {
	ewma := r.ewma[beaconReply.Replica]
	r.ewma[beaconReply.Replica] = 0.99*ewma + 0.01*float64(rdtsc.Cputicks()-uint64(beaconReply.Timestamp))
	return &Empty{}, nil
}

func (r *replica) Propose(ctx context.Context, prop *Proposal) (*Empty, error) {
	r.proposals <- prop
	return &Empty{}, nil
}
