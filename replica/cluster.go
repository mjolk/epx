package replica

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epx/rdtsc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//	"google.golang.org/grpc/credentials"
)

//TODO refactor connect sequence, let only one node connect to all the others; the streams are bidirectional,
//1 stream between each replica is enough => will consume only a connection per peer
//TODO make stream handling more robust

type Cluster interface {
	Replicas() []RemoteReplica
	Len() int
	Replica(id int32) RemoteReplica
	Start()
	Join([]RemoteReplica)
	ReplicaOrder() []int32
	InitReplicaOrder(Replica)
	UpdateReplicaOrder(Replica, []int32)
	PreAccept(bool, *PreAcceptance)
	ReplyAccept(int32, *AcceptanceReply)
	ReplyProposeTS(GrpcReplica_ProposeServer, *ProposalReplyTS)
	ReplyPreAccept(int32, *PreAcceptanceReply)
	ReplyPrepare(int32, *PreparationReply)
	ReplyTryPreAccept(int32, *TryPreAcceptanceReply)
	PreAcceptanceOk(int32, *PreAcceptanceOk)
	Commit(bool, *TryCommit, *TryCommitShort)
	Accept(bool, *Acceptance)
	SetReplicaOrder([]float64)
	Ping(*Beacon)
	Prepare(bool, int32, *Preparation)
	TryPreAccept(int32, *TryPreAcceptance)
	SetContext(context.Context)
	Context() context.Context
	ProposeStream(int32) GrpcReplica_ProposeClient
}

type cluster struct {
	replicas     []RemoteReplica
	badReplicas  []RemoteReplica
	replicaOrder []int32
	ctx          context.Context
}

func (c *cluster) SetContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *cluster) Context() context.Context {
	return c.ctx
}

func (c *cluster) Replicas() []RemoteReplica {
	return c.replicas
}

func (c *cluster) Len() int {
	return len(c.replicas)
}

//todo add timeout
func (c *cluster) Start() {
	replicaCount := len(c.replicas)
	doneChan := make(chan int, replicaCount)
	done := 0
	for _, replica := range c.replicas {
		go c.connect(replica, doneChan)
	}
	for done < replicaCount {
		done += <-doneChan
	}
}

func (c *cluster) Join(replicas []RemoteReplica) {
	for _, ereplica := range c.replicas {
		for _, newReplica := range replicas {
			if ereplica.Addr() == newReplica.Addr() {
				continue
			}
		}
	}
}

//never more than a couple dozen replicas so this is faster
//than a map
func (c *cluster) Replica(id int32) RemoteReplica {
	for _, replica := range c.replicas {
		if replica.Id() == id {
			return replica
		}
	}
	return nil
}

func NewCluster(ctx context.Context, addrs []string) (Cluster, error) {
	cnt := len(addrs)
	cluster := new(cluster)
	cluster.ctx = ctx
	if len(addrs) < 2 {
		return nil, errors.New("Need at least 3 peers")
	}
	for i, addr := range addrs {
		replica := NewRemoteReplica(int32(i), addr)
		cluster.replicas = append(cluster.replicas, replica)
	}
	cluster.replicaOrder = make([]int32, cnt)
	return cluster, nil
}

func (c *cluster) InitReplicaOrder(r Replica) {
	cnt := c.Len()
	for i := 0; i < cnt; i++ {
		c.replicaOrder[i] = int32((int(r.Id()) + 1 + i) % cnt)
	}
}

func (c *cluster) UpdateReplicaOrder(r Replica, quorum []int32) {
	aux := make([]int32, c.Len())
	i := 0
	for _, p := range quorum {
		if p == r.Id() {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range c.replicaOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	c.replicaOrder = aux
}

func (c *cluster) connect(replica RemoteReplica, done chan<- int) {
	var opts []grpc.DialOption
	//creds, err := credentials.NewClientTLSFromFile("mjolk.be.pem", "mjolk.be")
	//if err != nil {
	//	log.Infof("Failed to create TLS credentials")
	//}
	opts = append(opts, grpc.WithInsecure() /*grpc.WithTransportCredentials(creds)*/)
	conn, err := grpc.Dial(replica.Addr(), opts...)
	if err != nil {
		log.Infof("fail to dial")
	}
	replica.SetClient(NewGrpcReplicaClient(conn))
	done <- 1
}

func (c *cluster) PreAccept(thrifty bool, preAccept *PreAcceptance) {
	cLen := c.Len()
	n := cLen - 1
	if thrifty {
		n = cLen / 2
	}
	sent := 0
	for q := 0; q < cLen; q++ {
		replica := c.replicaOrder[q]
		go c.Replica(replica).PreAcceptStream(c.ctx).Send(preAccept)
		sent++
		if sent >= n {
			break
		}
	}
}

func (c *cluster) Commit(thrifty bool, commit *TryCommit, commitShort *TryCommitShort) {
	cLen := c.Len()
	n := cLen - 1
	n2 := cLen / 2
	sent := 0
	for q := 0; q < n; q++ {
		if thrifty && sent >= n2 {
			replica := c.replicaOrder[q]
			go c.Replica(replica).CommitStream(c.ctx).Send(commit)
		} else {
			replica := c.replicaOrder[q]
			go c.Replica(replica).CommitShortStream(c.ctx).Send(commitShort)
			sent++
		}
	}
}

func (c *cluster) Accept(thrifty bool, accept *Acceptance) {
	cLen := c.Len()
	n := cLen - 1
	if thrifty {
		n = cLen / 2
	}

	sent := 0
	for q := 0; q < cLen-1; q++ {
		replica := c.replicaOrder[q]
		log.WithFields(log.Fields{
			"toreplica": replica,
		}).Info("accept to")
		go c.Replica(replica).AcceptStream(c.ctx).Send(accept)
		sent++
		if sent >= n {
			break
		}
	}
}

func (c *cluster) SetReplicaOrder(ewma []float64) {
	cLen := c.Len()

	for i := 0; i < cLen-1; i++ {
		min := i
		for j := i + 1; j < cLen-1; j++ {
			if ewma[c.replicaOrder[j]] < ewma[c.replicaOrder[min]] {
				min = j
			}
		}
		aux := c.replicaOrder[i]
		c.replicaOrder[i] = c.replicaOrder[min]
		c.replicaOrder[min] = aux
	}
	log.WithFields(log.Fields{
		"replicaorder": c.replicaOrder,
		"ewma":         ewma,
	}).Info("setting replicaorder -->>")
}

//OUT STREAMS :: SEND -> RECEIVE

func (c *cluster) ReplicaOrder() []int32 {
	return c.replicaOrder
}

func (c *cluster) ProposeStream(replica int32) GrpcReplica_ProposeClient {
	return c.Replica(replica).ProposeStream(c.ctx)
}

//IN STREAMS :: RECEIVE -> SEND

//TODO switch to client proposal
func (c *cluster) ReplyProposeTS(stream GrpcReplica_ProposeServer, reply *ProposalReplyTS) {
	log.Info("send propose reply ------>>>")
	if err := stream.Send(reply); err != nil {
		log.Info("err sending proposal reply")
	}
}

func (c *cluster) ReplyPreAccept(replica int32, preply *PreAcceptanceReply) {
	reply := new(PreAcceptReply)
	reply.Type = PreAcceptReply_PREACCEPTREPLY
	reply.PreAcceptanceReply = preply
	if err := c.Replica(replica).IPreAcceptStream().Send(reply); err != nil {
		log.Info("err sending preaccept reply")
	}
}

func (c *cluster) PreAcceptanceOk(replica int32, poreply *PreAcceptanceOk) {
	reply := new(PreAcceptReply)
	reply.Type = PreAcceptReply_PREACCEPTOK
	reply.PreAcceptanceOk = poreply
	if err := c.Replica(replica).IPreAcceptStream().Send(reply); err != nil {
		log.Info("err sending preaccept reply")
	}
}

func (c *cluster) ReplyAccept(replica int32, reply *AcceptanceReply) {
	if err := c.Replica(replica).IAcceptStream().Send(reply); err != nil {
		log.Info("err sending accept reply")
	}
}

func (c *cluster) ReplyPrepare(replica int32, reply *PreparationReply) {
	log.WithFields(log.Fields{
		"replica": replica,
		"reply":   reply,
	}).Info("reply preparation")
	if err := c.Replica(replica).IPrepareStream().Send(reply); err != nil {
		log.Info("err sending prepare reply")
	}
}

func (c *cluster) ReplyTryPreAccept(replica int32, reply *TryPreAcceptanceReply) {
	if err := c.Replica(replica).ITryPreAcceptStream().Send(reply); err != nil {
		log.Info("err sending trypreaccept reply")
	}
}

func (c *cluster) Ping(beacon *Beacon) {
	beacon.Timestamp = rdtsc.Cputicks()
	cLen := int32(c.Len())
	var q int32
	for ; q < cLen; q++ {
		if q == beacon.Replica {
			continue
		}
		r := q
		go c.Replica(r).PingStream(c.ctx).
			Send(beacon)
	}
}

func (c *cluster) Prepare(thrifty bool, replica int32, preparation *Preparation) {
	cLen := c.Len()
	n := cLen - 1
	if thrifty {
		n = cLen / 2
	}
	q := replica
	for sent := 0; sent < n; {
		q = (q + 1) % int32(cLen)
		if q == replica {
			break
		}
		r := c.replicaOrder[q]
		log.WithFields(log.Fields{
			"replica to": r,
		}).Info("PREPARE TO")
		go c.Replica(r).
			PrepareStream(c.ctx).
			Send(preparation)
		sent++
	}
}

func (c *cluster) TryPreAccept(replica int32, try *TryPreAcceptance) {
	cLen := c.Len()
	for q := 0; q < cLen; q++ {
		if int32(q) == replica {
			continue
		}
		replica := c.replicaOrder[q]
		go c.Replica(replica).TryPreAcceptStream(c.ctx).
			Send(try)
	}
}
