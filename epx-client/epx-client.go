package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/mjolk/epx/replica"
	"github.com/mjolk/epx/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"os"
	"time"
)

var addrs = []string{
	"localhost:10001",
	"localhost:10002",
	"localhost:10003",
}

var ports = []string{
	":10001",
	":10002",
	":10003",
}

var successful []int

var rarray []int

func main() {
	app := cli.NewApp()
	app.Name = "client"
	app.Usage = "run client"
	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "reqs, r",
			Value: 100,
			Usage: "number of requests",
		},
		cli.IntFlag{
			Name:  "writes, w",
			Value: 100,
			Usage: "percentage of updates default 100%",
		},
		cli.BoolFlag{
			Name:  "fast, f",
			Usage: "Fast paxos, send message direct to all replicas",
		},
		cli.IntFlag{
			Name:  "rounds, rds",
			Value: 1,
			Usage: "Split total number of requests into rds rounds, do rounds dequentially",
		},
		cli.BoolFlag{
			Name:  "check",
			Usage: "check that every expected reply was only received once",
		},
		cli.IntFlag{
			Name:  "eps, e",
			Value: 0,
			Usage: "Send eps more messages per round than the client will wait for (to discount stragglers).",
		},
		cli.IntFlag{
			Name:  "conflicts, c",
			Value: 5,
			Usage: "Percentage of conflicts. Defaults to 0%",
		},
	}
	app.Action = func(c *cli.Context) {
		f, err := os.OpenFile("logclient.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		log.SetOutput(f)
		log.Info("starting client")
		go run(c)
		start()
	}
	app.Run(os.Args)
}

var requests int

func run(c *cli.Context) {
	requests := c.Int("reqs")
	rounds := c.Int("rounds")
	eps := c.Int("eps")
	conflicts := c.Int("conflicts")
	writes := c.Int("writes")
	fast := false
	rand.Seed(time.Now().Unix())

	if conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	cluster, err := replica.NewCluster(addrs)
	if err != nil {
		panic(err)
	}
	cluster.Start()

	log.Info("cluster started")

	N := cluster.Len()

	requestsPr := requests/rounds + eps
	varray := util.RandStrings(requestsPr)
	rarray := make([]int, requestsPr)
	karray := util.RandStrings(requestsPr)
	conflictKey := "/blabla/bla"
	put := make([]bool, requestsPr)
	perReplicaCount := make([]int, N)
	test := make(map[string]int, requestsPr)

	for i, key := range karray {
		r := rand.Intn(N)
		rarray[i] = r
		if i < requests/rounds {
			perReplicaCount[rarray[i]]++
		}

		if conflicts >= 0 {
			r = rand.Intn(100)
			if r < conflicts {
				karray[i] = conflictKey
			} else {
				test[key]++
			}
		}
		r = rand.Intn(100)
		if r < writes {
			put[i] = true
		} else {
			put[i] = false
		}
	}

	var id int32 = 0

	beforeTotal := time.Now()

	for j := 0; j < rounds; j++ {

		before := time.Now()

		for i := 0; i < requestsPr; i++ {
			proposal := &replica.Proposal{
				id,
				&replica.Command{replica.Command_PUT, "a", []byte("test")},
				time.Now().Unix(),
			}
			proposal.CommandId = id
			if put[i] {
				proposal.Command.Operation = replica.Command_PUT
			} else {
				proposal.Command.Operation = replica.Command_GET
			}
			proposal.Command.Key = karray[i]
			proposal.Command.Value = []byte(varray[i])
			//args.Timestamp = time.Now().UnixNano()
			if !fast {
				leader := rarray[i]
				log.WithFields(log.Fields{
					"proposal": proposal,
				}).Info("Sending proposal")
				go cluster.Propose(int32(leader), proposal)
			} else {
				//send to everyone
				for rep := 0; rep < N; rep++ {
					go cluster.Propose(int32(rep), proposal)
				}
			}
			id++
		}

		after := time.Now()

		log.WithFields(log.Fields{
			"time": after.Sub(before),
		}).Info("round done")
	}

	afterTotal := time.Now()
	log.WithFields(log.Fields{
		"time": afterTotal.Sub(beforeTotal),
	}).Info("Test done\n")

}

type clientServer struct {
	replies []*replica.ProposalReplyTS
}

var reps int

func (r *clientServer) ReplyProposeTS(ctx context.Context, propReplyTS *replica.ProposalReplyTS) (*replica.Empty, error) {
	r.replies = append(r.replies, propReplyTS)
	reps++
	log.WithFields(log.Fields{
		"replyValue": propReplyTS,
		"count":      reps,
	}).Info("Received reply")
	if reps == requests {
		log.WithFields(log.Fields{
			"count": requests,
		}).Info("Success")
	}
	return &replica.Empty{}, nil
}

func newClientServer() replica.GrpcProposeServer {
	return &clientServer{
		make([]*replica.ProposalReplyTS, 0),
	}
}

func start() {
	log.Info("setting up server")
	lis, err := net.Listen("tcp", ":10000")
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
	replica.RegisterGrpcProposeServer(grpcServer, newClientServer())
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}
