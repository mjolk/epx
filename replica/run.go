package replica

import (
	"errors"
	"time"
)

func (r *epaxosReplica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.beacon = false
	time.Sleep(1000 * 1000 * 1000)

	r.cluster.SetReplicaOrder(r.ewma)
}

var conflicted, weird, slow, happy int

func Start(id int32, port string, addr []string) (err error) {
	var cluster Cluster
	if cluster, err = NewCluster(addr); err != nil {
		return errors.New("need at least three replicas")
	}
	cluster.Start()

	replica := NewEpaxosReplica(id, port, cluster)
	replica.Start()
	return nil
}

func (r *epaxosReplica) run() {
	clusterSize := r.cluster.Len()

	if r.exec {
		go r.executeCommands()
	}

	if r.Id() == 0 {
		//init quorum read lease
		quorum := make([]int32, clusterSize/2+1)
		for i := 0; i <= clusterSize/2; i++ {
			quorum[i] = int32(i)
		}
		r.cluster.UpdateReplicaOrder(r, quorum)
	}

	slowClock := time.NewTicker(time.Millisecond * 150).C
	var fastClock <-chan time.Time

	//Enabled when batching for 5ms
	if MAX_BATCH > 100 {
		fastClock = time.NewTicker(time.Millisecond * 5).C
	}

	if r.beacon {
		go r.stopAdapting()
	}

	onOffProposeChan := r.proposals

	for {

		select {

		case proposal := <-onOffProposeChan:
			//got a Propose from a client
			r.propose(proposal)
			//deactivate new proposals channel to prioritize the handling of other protocol messages,
			//and to allow commands to accumulate for batching

		case <-fastClock:
			//activate new proposals channel
			onOffProposeChan = r.proposals

		case preparation := <-r.preparations:
			//got a Prepare message
			r.prepare(preparation)

		case preAcceptance := <-r.preAcceptances:
			//got a PreAccept message
			r.preAccept(preAcceptance)

		case acceptance := <-r.acceptances:
			//got an Accept message
			r.accept(acceptance)

		case commit := <-r.commits:
			//got a Commit message
			r.commit(commit)

		case commitShort := <-r.commitsShort:
			//got a Commit message
			r.commitShort(commitShort)

		case preparationReply := <-r.preparationReplies:
			//got a Prepare reply
			r.prepareReply(preparationReply)

		case preAcceptanceReply := <-r.preAcceptanceReplies:
			//got a PreAccept reply
			r.preAcceptReply(preAcceptanceReply)

		case preAcceptanceOk := <-r.preAcceptanceOks:
			//got a PreAccept reply
			r.preAcceptOK(preAcceptanceOk)

		case acceptanceReply := <-r.acceptanceReplies:
			//got an Accept reply
			r.acceptReply(acceptanceReply)
			break

		case tryPreAcceptance := <-r.tryPreAcceptances:
			r.tryPreAccept(tryPreAcceptance)
			break

		case tryPreAcceptanceReply := <-r.tryPreAcceptanceReplies:
			r.tryPreAcceptReply(tryPreAcceptanceReply)
			break

		case beacon := <-r.beacons:
			r.cluster.ReplyPing(r.id, beacon)
			break

		case <-slowClock:
			if r.beacon {
				for q := int32(0); q < int32(clusterSize); q++ {
					if q == r.id {
						continue
					}
					r.cluster.Ping(q)
				}
			}
			break
			/*	case <-r.OnClientConnect:
				log.Printf("weird %d; conflicted %d; slow %d; happy %d\n", weird, conflicted, slow, happy)
				weird, conflicted, slow, happy = 0, 0, 0, 0*/

		case iid := <-r.recoveryInstances:
			r.startRecoveryForInstance(iid.replica, iid.instance)
		}
	}
}
