package replica

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epx/rdtsc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//	"google.golang.org/grpc/credentials"
)

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
	Commit(bool, *TryCommit, *TryCommitShort)
	Accept(bool, *Acceptance)
	SetReplicaOrder([]float64)
	ProposeStream(int32) GrpcReplica_ProposeStreamClient
	Propose(int32, *Proposal)
	ReplyProposeTS(GrpcReplica_ProposeStreamServer, *ProposalReplyTS)
	ReplyPreAccept(int32, *PreAcceptanceReply)
	PreAcceptanceOk(int32, *PreAcceptanceOk)
	ReplyAccept(int32, *AcceptanceReply)
	ReplyPrepare(int32, *PreparationReply)
	ReplyTryPreAccept(int32, *TryPreAcceptanceReply)
	Ping(*Beacon)
	ReplyPing(int32, *BeaconReply)
	Prepare(bool, int32, *Preparation)
	TryPreAccept(int32, *TryPreAcceptance)
}

type client struct {
	address string
	GrpcProposeClient
}

type cluster struct {
	replicas     []RemoteReplica
	badReplicas  []RemoteReplica
	replicaOrder []int32
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

func NewCluster(addrs []string) (Cluster, error) {
	cnt := len(addrs)
	cluster := new(cluster)
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
	//	log.Fatalf("Failed to create TLS credentials")
	//}
	opts = append(opts, grpc.WithInsecure() /*grpc.WithTransportCredentials(creds)*/)
	conn, err := grpc.Dial(replica.Addr(), opts...)
	if err != nil {
		log.Fatalf("fail to dial")
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
	ctx := context.Background()
	for q := 0; q < cLen; q++ {
		replica := c.replicaOrder[q]
		go c.Replica(replica).PreAccept(ctx, preAccept)
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
	ctx := context.Background()
	for q := 0; q < n; q++ {
		if thrifty && sent >= n2 {
			c.Replica(c.replicaOrder[q]).Commit(ctx, commit)
		} else {
			replica := c.replicaOrder[q]
			go c.Replica(replica).CommitShort(ctx, commitShort)
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
	ctx := context.Background()
	for q := 0; q < cLen-1; q++ {
		replica := c.replicaOrder[q]
		go c.Replica(replica).Accept(ctx, accept)
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

func (c *cluster) ReplicaOrder() []int32 {
	return c.replicaOrder
}

func (c *cluster) ReplyProposeTS(stream GrpcReplica_ProposeStreamServer,
	reply *ProposalReplyTS) {
	//send to client who proposed
	//c.client.ReplyProposeTS(context.Background(), reply)
	if err := stream.Send(reply); err != nil {
		log.Fatal("err sending proposal reply")
	}
}

func (c *cluster) ProposeStream(replica int32) GrpcReplica_ProposeStreamClient {
	return c.Replica(replica).ProposeStreamServer()
}

func (c *cluster) Propose(replicaId int32, proposal *Proposal) {
	c.Replica(replicaId).Propose(context.Background(), proposal.ClientProposal)
}

func (c *cluster) ReplyPreAccept(replicaId int32, reply *PreAcceptanceReply) {
	c.Replica(replicaId).ReplyPreAccept(context.Background(), reply)
}

func (c *cluster) PreAcceptanceOk(replicaId int32, reply *PreAcceptanceOk) {
	c.Replica(replicaId).PreAcceptOK(context.Background(), reply)
}

func (c *cluster) ReplyAccept(replicaId int32, reply *AcceptanceReply) {
	c.Replica(replicaId).ReplyAccept(context.Background(), reply)
}

func (c *cluster) ReplyPrepare(replicaId int32, reply *PreparationReply) {
	c.Replica(replicaId).ReplyPrepare(context.Background(), reply)
}

func (c *cluster) ReplyTryPreAccept(replicaId int32, reply *TryPreAcceptanceReply) {
	c.Replica(replicaId).ReplyTryPreAccept(context.Background(), reply)
}

func (c *cluster) Ping(beacon *Beacon) {
	beacon.Timestamp = rdtsc.Cputicks()
	cLen := int32(c.Len())
	var q int32
	ctx := context.Background()
	for ; q < cLen; q++ {
		if q == beacon.Replica {
			continue
		}
		go func(r int32) {
			c.Replica(r).Ping(ctx, beacon)
		}(q)
	}
}

func (c *cluster) ReplyPing(replicaId int32, beaconReply *BeaconReply) {
	c.Replica(replicaId).ReplyPing(context.Background(), beaconReply)
}

func (c *cluster) Prepare(thrifty bool, replicaId int32, preparation *Preparation) {
	cLen := c.Len()
	n := cLen - 1
	if thrifty {
		n = cLen / 2
	}
	q := replicaId
	context := context.Background()
	for sent := 0; sent < n; {
		q = (q + 1) % int32(cLen)
		if q == replicaId {
			break
		}
		c.Replica(c.replicaOrder[q]).Prepare(context, preparation)
		sent++
	}
}

func (c *cluster) TryPreAccept(replicaId int32, try *TryPreAcceptance) {
	cLen := c.Len()
	ctx := context.Background()
	for q := 0; q < cLen; q++ {
		if int32(q) == replicaId {
			continue
		}
		replica := c.replicaOrder[q]
		go c.Replica(replica).TryPreAccept(ctx, try)
	}
}
