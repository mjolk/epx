package replica

/***********************************
   Command execution thread        *
************************************/

import (
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

func (c *Command) Execute(store Store) string {
	//fmt.Printf("Executing (%d, %d)\n", c.K, c.V)

	//var key, value [8]byte

	//    st.mutex.Lock()
	//    defer st.mutex.Unlock()

	switch c.Operation {
	case Command_PUT:
		/*
		   binary.LittleEndian.PutUint64(key[:], uint64(c.K))
		   binary.LittleEndian.PutUint64(value[:], uint64(c.V))
		   st.DB.Set(key[:], value[:], nil)
		*/

		store.Put(c.Key, c.Value)
		return c.Value

	case Command_GET:
		return store.Get(c.Key)
	}

	return NIL
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
	return r.strongconnect(root, &index)
}

func (r *epaxosReplica) strongconnect(v *Instance, index *int) bool {
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
				time.Sleep(1000 * 1000)
			}
			/*        if !conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
			          continue
			          }
			*/
			if r.instanceSpace[q][i].status == Status_EXECUTED {
				continue
			}
			for r.instanceSpace[q][i].status != Status_COMMITTED {
				time.Sleep(1000 * 1000)
			}
			w := r.instanceSpace[q][i]

			if w.index == 0 {
				//e.strongconnect(w, index)
				if !r.strongconnect(w, index) {
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
				time.Sleep(1000 * 1000)
			}
			for idx := 0; idx < len(w.commands); idx++ {
				val := w.commands[idx].Execute(r.store)
				if r.dreply && w.lb != nil && w.lb.clientProposals != nil {
					r.cluster.ReplyProposeTS(
						&ProposalReplyTS{
							true,
							w.lb.clientProposals[idx].CommandId,
							val,
							w.lb.clientProposals[idx].Timestamp})
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

func (r *epaxosReplica) executeCommands() {
	const SLEEP_TIME_NS = 1e6
	cLen := r.cluster.Len()
	problemInstance := make([]int32, cLen)
	timeout := make([]uint64, cLen)
	for q := 0; q < cLen; q++ {
		problemInstance[q] = -1
		timeout[q] = 0
	}

	executed := false
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
				if inst == problemInstance[q] {
					timeout[q] += SLEEP_TIME_NS
					if timeout[q] >= COMMIT_GRACE_PERIOD {
						r.recoveryInstances <- &instanceId{int32(q), inst}
						timeout[q] = 0
					}
				} else {
					problemInstance[q] = inst
					timeout[q] = 0
				}
				if r.instanceSpace[q][inst] == nil {
					continue
				}
				break
			}
			if ok := r.executeCommand(int32(q), inst); ok {
				executed = true
				if inst == r.executedUpTo[q]+1 {
					r.executedUpTo[q] = inst
				}
			}
		}
	}
	if !executed {
		time.Sleep(SLEEP_TIME_NS)
	}
	//log.Println(r.ExecedUpTo, " ", r.crtInstance)
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
