package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/mjolk/epx/replica"
	"github.com/mjolk/epx/util"
	"golang.org/x/net/context"
	"io"
	"math/rand"
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

var rarray []int

func main() {
	app := cli.NewApp()
	app.Name = "client"
	app.Usage = "run client"
	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "reqs, r",
			Value: 5000,
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
		log.Info("starting client")
		run(c)
	}
	app.Run(os.Args)
}

var requests int

var cnt int

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

	root := context.Background()
	ctx, _ := context.WithCancel(root)
	cluster, err := replica.NewCluster(ctx, addrs)
	if err != nil {
		panic(err)
	}

	log.Info("starting cluster")
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

	replies := make(chan *replica.ProposalReplyTS)

	var id int32 = 0
	for i := 0; i < N; i++ {
		stream := cluster.ProposeStream(int32(i))
		go func(replica int32) {
			for {
				reply, err := stream.Recv()
				if err == io.EOF {
					log.Info("EOF")
					return
				}
				if err != nil {
					log.Info("could not read from stream")
				}
				replies <- reply
			}
		}(int32(i))
	}

	beforeTotal := time.Now()

	for j := 0; j < rounds; j++ {

		for i := 0; i < requestsPr; i++ {
			proposal := &replica.ClientProposal{
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
				go cluster.ProposeStream(int32(leader)).Send(proposal)
			} else {
				//send to everyone
				for rep := 0; rep < N; rep++ {
					go cluster.ProposeStream(int32(rep)).Send(proposal)
				}
			}
			id++
		}
	}

	for {
		select {
		case reply := <-replies:
			cnt++
			log.WithFields(log.Fields{
				"count":  cnt,
				"number": requests,
				"reply":  reply,
			}).Info("received reply")

		default:
			if cnt == requests {
				afterTotal := time.Now()
				log.WithFields(log.Fields{
					"time": afterTotal.Sub(beforeTotal),
				}).Info("Test done\n")
				return
			}

		}

	}

}
