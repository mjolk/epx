package replica

/***********************************
   Command execution thread        *
************************************/

import (
	log "github.com/Sirupsen/logrus"
	"github.com/mjolk/epx/bloomfilter"
	"sort"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

func conflict(gamma *Command, delta *Command) bool {
	if gamma.Key == delta.Key {
		if gamma.Operation == Command_PUT || delta.Operation == Command_PUT {
			return true
		}
	}
	return false
}

func (c *Command) Execute(store Store) []byte {
	log.WithFields(log.Fields{
		"key":   c.Key,
		"value": c.Value,
	}).Info("executing on store")

	switch c.Operation {
	case Command_PUT:
		store.Put(NewKeyValue(c.Key, c.Value))
		return c.Value

	case Command_GET:
		return store.Get(c.Key).Value()
	}

	return []byte{}
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (r *epaxosReplica) executeCommand(replica int32, instance int32) bool {
	if r.instanceSpace[replica][instance] == nil {
		return false
	}
	inst := r.instanceSpace[replica][instance]
	if inst.status == Status_EXECUTED {
		return true
	}
	if inst.status != Status_COMMITTED {
		return false
	}

	if !r.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (r *epaxosReplica) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return r.strongConnect(root, &index)
}

func (r *epaxosReplica) strongConnect(v *Instance, index *int) bool {
	v.index = *index
	v.lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v
	cLen := r.cluster.Len()

	for q := int32(0); q < int32(cLen); q++ {
		inst := v.deps[q]
		for i := r.executedUpTo[q] + 1; i <= inst; i++ {
			for r.instanceSpace[q][i] == nil || r.instanceSpace[q][i].commands == nil || v.commands == nil {
				time.Sleep(time.Millisecond)
			}
			/*        if !conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
			          continue
			          }
			*/
			if r.instanceSpace[q][i].status == Status_EXECUTED {
				continue
			}
			for r.instanceSpace[q][i].status != Status_COMMITTED {
				time.Sleep(time.Millisecond)
			}
			w := r.instanceSpace[q][i]

			if w.index == 0 {
				//e.strongconnect(w, index)
				if !r.strongConnect(w, index) {
					stackLen := len(stack)
					for j := l; j < stackLen; j++ {
						stack[j].index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.lowlink < v.lowlink {
					v.lowlink = w.lowlink
				}
			} else { //if inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.index < v.lowlink {
					v.lowlink = w.index
				}
			}
		}
	}

	if v.lowlink == v.index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.commands == nil {
				time.Sleep(time.Millisecond)
			}
			for idx, cmd := range w.commands {
				val := cmd.Execute(r.store)
				if r.dreply && w.lb != nil && w.lb.clientProposals != nil {
					indx := idx
					go r.cluster.ReplyProposeTS(
						w.lb.clientProposals[indx].stream,
						&ProposalReplyTS{
							true,
							w.lb.clientProposals[indx].CommandId,
							val,
							w.lb.clientProposals[indx].Timestamp})
				}
			}
			w.status = Status_EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].seq < na[j].seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}

func (r *epaxosReplica) sweepInstanceSpace() {
	cLen := r.cluster.Len()
	for q := 0; q < cLen; q++ {
		inst := int32(0)
		for inst = r.executedUpTo[q] + 1; inst < r.crtInstance[q]; inst++ {
			if r.instanceSpace[q][inst] != nil && r.instanceSpace[q][inst].status == Status_EXECUTED {
				if inst == r.executedUpTo[q]+1 {
					r.executedUpTo[q] = inst
				}
				continue
			}
			if r.instanceSpace[q][inst] == nil || r.instanceSpace[q][inst].status != Status_COMMITTED {
				if inst == r.problemInstances[q] {
					r.clusterTimeouts[q] += time.Millisecond
					if r.clusterTimeouts[q] >= COMMIT_GRACE_PERIOD {
						r.recoveryInstances <- &instanceId{int32(q), inst}
						r.clusterTimeouts[q] = 0
					}
				} else {
					r.problemInstances[q] = inst
					r.clusterTimeouts[q] = 0
				}
				if r.instanceSpace[q][inst] == nil {
					continue
				}
				break
			}
			if ok := r.executeCommand(int32(q), inst); ok {
				if inst == r.executedUpTo[q]+1 {
					r.executedUpTo[q] = inst
				}
			}
		}
	}
}

func (r *epaxosReplica) executeCommands() {
	cLen := r.cluster.Len()
	r.problemInstances = make([]int32, cLen)
	r.clusterTimeouts = make([]time.Duration, cLen)
	for q := 0; q < cLen; q++ {
		r.problemInstances[q] = -1
		r.clusterTimeouts[q] = 0
	}
	sweeper := time.NewTicker(time.Millisecond * 5).C

	for {
		select {
		case <-sweeper:
			r.sweepInstanceSpace()
		}
	}
	log.Info("command execution terminated")
}

func bfFromCommands(cmds []Command) *bloomfilter.Bloomfilter {
	if cmds == nil {
		return nil
	}

	bf := bloomfilter.NewPowTwo(bf_PT, BF_K)

	//for i, cmd := range cmds {
	//	bf.AddUint64(uint64(cmd.Key))
	//}

	return bf
}
