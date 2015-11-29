package main

import (
	"github.com/codegangsta/cli"
	"github.com/mjolk/epaxos_grpc/replica"
	"os"
)

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
			Value: false,
			Usage: "Fast paxos, send message direct to all replicas",
		},
		cli.IntFlag{
			Name:  "rounds, rds",
			Value: 1,
			Usage: "Split total number of requests into rds rounds, do rounds dequentially",
		},
		cli.BoolFlag{
			Name:  "check",
			Value: false,
			Usage: "check that every expected reply was only received once",
		},
		cli.IntFlag{
			Name:  "eps, e",
			Value: 0,
			Usage: "Send eps more messages per round than the client will wait for (to discount stragglers).",
		},
		cli.IntFlag{
			Name:  "conflicts, c",
			Value: -1,
			Usage: "Percentage of conflicts. Defaults to 0%",
		},
		cli.IntFlag{
			Name:  "zipfians, s",
			Value: 2,
			Usage: "Zipfian s parameter",
		},
		cli.IntFlag{
			Name:  "zipfianv, v",
			Value: 1,
			Usage: "Zipfian v parameter",
		},
	}
	app.Action = func(c *cli.Context) {
		setup()
	}
	app.Run(os.Args)
}

func setup() {
	random := rand.New(rand.NewSource(42))
	zipf
}
