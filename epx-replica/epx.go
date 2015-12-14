package main

import (
	"github.com/codegangsta/cli"
	"github.com/mjolk/epx/replica"
	"golang.org/x/net/context"
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
		ctx := context.Background()
		ctxc, _ := context.WithCancel(ctx)
		if err := replica.Start(ctxc, int32(id), ports[id], addrs,
			replica.NewTestStore(5000)); err != nil {
			panic(err)
		}
	}
	app.Run(os.Args)
}
