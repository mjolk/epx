package main

import (
	"github.com/codegangsta/cli"
	"github.com/mjolk/epaxos_grpc/replica"
	"os"
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

func main() {
	app := cli.NewApp()
	app.Name = "replica"
	app.Usage = "run replica"
	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "replica, i",
			Value: 0,
			Usage: "replica id",
		},
	}
	app.Action = func(c *cli.Context) {
		var id int = c.Int("replica")
		if err := replica.Start(int32(id), ports[id], addrs); err != nil {
			panic(err)
		}
	}
	app.Run(os.Args)
}
