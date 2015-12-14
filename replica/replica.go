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
	Ping(GrpcReplica_PingServer) error
	Propose(GrpcReplica_ProposeServer) error
	ProposeAndRead(GrpcReplica_ProposeAndReadServer) error
	Prepare(GrpcReplica_PrepareServer) error
	TryPreAccept(GrpcReplica_TryPreAcceptServer) error
	PreAccept(GrpcReplica_PreAcceptServer) error
	Accept(GrpcReplica_AcceptServer) error
	Commit(GrpcReplica_CommitServer) error
	CommitShort(GrpcReplica_CommitShortServer) error
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
	Ping(context.Context, ...grpc.CallOption) (GrpcReplica_PingClient, error)
	Propose(context.Context, ...grpc.CallOption) (GrpcReplica_ProposeClient, error)
	ProposeAndRead(context.Context, ...grpc.CallOption) (GrpcReplica_ProposeAndReadClient, error)
	Prepare(context.Context, ...grpc.CallOption) (GrpcReplica_PrepareClient, error)
	TryPreAccept(context.Context, ...grpc.CallOption) (GrpcReplica_TryPreAcceptClient, error)
	PreAccept(context.Context, ...grpc.CallOption) (GrpcReplica_PreAcceptClient, error)
	Accept(context.Context, ...grpc.CallOption) (GrpcReplica_AcceptClient, error)
	Commit(context.Context, ...grpc.CallOption) (GrpcReplica_CommitClient, error)
	CommitShort(context.Context, ...grpc.CallOption) (GrpcReplica_CommitShortClient, error)
	ProposeStream(context.Context) GrpcReplica_ProposeClient
	PingStream(context.Context) GrpcReplica_PingClient
	AcceptStream(context.Context) GrpcReplica_AcceptClient
	PreAcceptStream(context.Context) GrpcReplica_PreAcceptClient
	TryPreAcceptStream(context.Context) GrpcReplica_TryPreAcceptClient
	PrepareStream(context.Context) GrpcReplica_PrepareClient
	CommitStream(context.Context) GrpcReplica_CommitClient
	CommitShortStream(context.Context) GrpcReplica_CommitShortClient
	SetIPrepareStream(GrpcReplica_PrepareServer)
	IPrepareStream() GrpcReplica_PrepareServer
	IAcceptStream() GrpcReplica_AcceptServer
	SetIAcceptStream(GrpcReplica_AcceptServer)
	IPreAcceptStream() GrpcReplica_PreAcceptServer
	SetIPreAcceptStream(GrpcReplica_PreAcceptServer)
	ITryPreAcceptStream() GrpcReplica_TryPreAcceptServer
	SetITryPreAcceptStream(GrpcReplica_TryPreAcceptServer)
}

//float64func NewReplica(id int32)

type replica struct {
	id        int32
	address   string
	cluster   Cluster
	store     Store
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
	id                  int32
	address             string
	shutdown            chan bool
	thrifty             bool
	exec                bool
	dreply              bool
	beacon              bool
	durable             bool
	proposeStream       GrpcReplica_ProposeClient
	pingStream          GrpcReplica_PingClient
	preAcceptStream     GrpcReplica_PreAcceptClient
	tryPreAcceptStream  GrpcReplica_TryPreAcceptClient
	acceptStream        GrpcReplica_AcceptClient
	prepareStream       GrpcReplica_PrepareClient
	commitStream        GrpcReplica_CommitClient
	commitShortStream   GrpcReplica_CommitShortClient
	iPreAcceptStream    GrpcReplica_PreAcceptServer
	iAcceptStream       GrpcReplica_AcceptServer
	iPrepareStream      GrpcReplica_PrepareServer
	iTryPreAcceptStream GrpcReplica_TryPreAcceptServer
	GrpcReplicaClient
}

type Proposal struct {
	*ClientProposal
	stream GrpcReplica_ProposeServer
}

func NewReplica(id int32, address string, cluster Cluster) *replica {
	replicaCnt := cluster.Len()
	replica := &replica{
		id:        id,
		address:   address,
		cluster:   cluster,
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

func (r *remoteReplica) SetIPrepareStream(stream GrpcReplica_PrepareServer) {
	r.iPrepareStream = stream
}

func (r *remoteReplica) IPrepareStream() GrpcReplica_PrepareServer {
	return r.iPrepareStream
}
func (r *remoteReplica) IAcceptStream() GrpcReplica_AcceptServer {
	return r.iAcceptStream
}

func (r *remoteReplica) SetIAcceptStream(stream GrpcReplica_AcceptServer) {
	r.iAcceptStream = stream
}

func (r *remoteReplica) IPreAcceptStream() GrpcReplica_PreAcceptServer {
	return r.iPreAcceptStream
}

func (r *remoteReplica) SetIPreAcceptStream(stream GrpcReplica_PreAcceptServer) {
	r.iPreAcceptStream = stream
}

func (r *remoteReplica) SetITryPreAcceptStream(stream GrpcReplica_TryPreAcceptServer) {
	r.iTryPreAcceptStream = stream
}

func (r *remoteReplica) ITryPreAcceptStream() GrpcReplica_TryPreAcceptServer {
	return r.iTryPreAcceptStream
}

func (r *remoteReplica) ProposeStream(ctx context.Context) GrpcReplica_ProposeClient {
	var err error
	if r.proposeStream == nil {
		c, _ := context.WithCancel(ctx)
		r.proposeStream, err = r.Propose(c)
		if err != nil {
			log.Info("could not create proposestream")
		}
	}

	return r.proposeStream
}

func (r *remoteReplica) PingStream(ctx context.Context) GrpcReplica_PingClient {
	var err error
	if r.pingStream == nil {
		c, _ := context.WithCancel(ctx)
		r.pingStream, err = r.Ping(c)
		if err != nil {
			log.Info("Could not create ping stream")
		}
		if node := ReplicaFromContext(ctx); node != nil {
			go func() {
				for {
					reply, err := r.pingStream.Recv()
					if err == io.EOF {
						log.Info("pingstream closed")
					}
					if err != nil {
						log.Info("could not receive ping replies")
					}
					ewma := node.ewma[reply.Replica]
					node.ewma[reply.Replica] = 0.99*ewma + 0.01*float64(rdtsc.Cputicks()-uint64(reply.Timestamp))
				}
			}()
		}

	}
	return r.pingStream
}

func (r *remoteReplica) PrepareStream(ctx context.Context) GrpcReplica_PrepareClient {
	var err error
	if r.prepareStream == nil {
		c, _ := context.WithCancel(ctx)
		r.prepareStream, err = r.Prepare(c)
		if err != nil {
			log.Info("Could not create prepare stream")
		}
		if node := ReplicaFromContext(ctx); node != nil {
			go func() {
				for {
					reply, err := r.prepareStream.Recv()
					if err == io.EOF {
						log.Info("preparestream closed")
					}
					if err != nil {
						log.Info("could not receive proposal replies")
					}
					node.preparationReplies <- reply
				}
			}()
		}
	}
	return r.prepareStream
}

func (r *remoteReplica) AcceptStream(ctx context.Context) GrpcReplica_AcceptClient {
	var err error
	if r.acceptStream == nil {
		c, _ := context.WithCancel(ctx)
		r.acceptStream, err = r.Accept(c)
		if err != nil {
			log.Info("Could not create accept stream")
		}
		if node := ReplicaFromContext(ctx); node != nil {
			go func() {
				for {
					reply, err := r.acceptStream.Recv()
					if err == io.EOF {
						log.Info("acceptstream closed")
					}
					if err != nil {
						log.WithFields(log.Fields{
							"err": err,
						}).Info("could not receive accept replies")
					}
					node.acceptanceReplies <- reply
				}
			}()
		}
	}
	return r.acceptStream
}

func (r *remoteReplica) PreAcceptStream(ctx context.Context) GrpcReplica_PreAcceptClient {
	var err error
	if r.preAcceptStream == nil {
		c, _ := context.WithCancel(ctx)
		r.preAcceptStream, err = r.PreAccept(c)
		if err != nil {
			log.Info("Could not create preaccept stream")
		}
		if node := ReplicaFromContext(ctx); node != nil {
			go func() {
				for {
					reply, err := r.preAcceptStream.Recv()
					if err == io.EOF {
						log.Info("preacceptstream closed")
					}
					if err != nil {
						log.Info("could not receive preaccept replies")
					}
					switch reply.Type {
					case PreAcceptReply_PREACCEPTOK:
						node.preAcceptanceOks <- reply.PreAcceptanceOk
					case PreAcceptReply_PREACCEPTREPLY:
						node.preAcceptanceReplies <- reply.PreAcceptanceReply
					}
				}
			}()
		}
	}
	return r.preAcceptStream
}

func (r *remoteReplica) TryPreAcceptStream(ctx context.Context) GrpcReplica_TryPreAcceptClient {
	var err error
	if r.tryPreAcceptStream == nil {
		c, _ := context.WithCancel(ctx)
		r.tryPreAcceptStream, err = r.TryPreAccept(c)
		if err != nil {
			log.Info("Could not create trypreaccept stream")
		}
		if node := ReplicaFromContext(ctx); node != nil {
			go func() {
				for {
					reply, err := r.tryPreAcceptStream.Recv()
					if err == io.EOF {
						log.Info("tryPreacceptstream closed")
					}
					if err != nil {
						log.Info("could not receive trypreaccept replies")
					}
					node.tryPreAcceptanceReplies <- reply
				}
			}()
		}
	}
	return r.tryPreAcceptStream
}

func (r *remoteReplica) CommitStream(ctx context.Context) GrpcReplica_CommitClient {
	var err error
	if r.commitStream == nil {
		c, _ := context.WithCancel(ctx)
		r.commitStream, err = r.Commit(c)
		if err != nil {
			log.Info("Could not create commit stream")
		}
	}
	return r.commitStream
}

func (r *remoteReplica) CommitShortStream(ctx context.Context) GrpcReplica_CommitShortClient {
	var err error
	if r.commitShortStream == nil {
		c, _ := context.WithCancel(ctx)
		r.commitShortStream, err = r.CommitShort(c)
		if err != nil {
			log.Info("Could not create commitshort stream")
		}
	}
	return r.commitShortStream
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

func (r *replica) Ewma() []float64 {
	return r.ewma
}

func (r *replica) Ping(stream GrpcReplica_PingServer) error {
	for {
		ping, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&BeaconReply{ping.Timestamp, r.id}); err != nil {
			return err
		}
	}
}

func (r *remoteReplica) SetClient(client GrpcReplicaClient) {
	r.GrpcReplicaClient = client
}

func (r *replica) Propose(stream GrpcReplica_ProposeServer) error {
	for {
		p, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		r.proposals <- &Proposal{p, stream}
	}
}
