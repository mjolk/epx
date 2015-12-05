package replica

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"time"
)

var slowTick *time.Ticker
var fastTick *time.Ticker

func (r *epaxosReplica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	slowTick.Stop()
	time.Sleep(1000 * 1000 * 1000)

	r.cluster.SetReplicaOrder(r.ewma)
}

var conflicted, weird, slow, happy int

func Start(id int32, port string, addr []string, client string) (err error) {
	log.WithFields(log.Fields{
		"replicaId": id,
		"port":      port,
	}).Info("Started replica")
	var cluster Cluster
	if cluster, err = NewCluster(addr); err != nil {
		return errors.New("need at least three replicas")
	}

	cluster.Client(client)
	cluster.Start()

	replica := NewEpaxosReplica(id, port, cluster)
	replica.Start()

	return nil
}

func (r *epaxosReplica) run() {
	clusterSize := r.cluster.Len()
	log.WithFields(log.Fields{
		"cluster": clusterSize,
		"replica": r.id,
	}).Info("running replica")
	r.cluster.InitReplicaOrder(r)

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

	slowTick = time.NewTicker(time.Millisecond * 150)
	fastTick = time.NewTicker(time.Millisecond * 5)
	slow := slowTick.C
	var fast <-chan time.Time

	//Enabled when batching for 5ms
	if MAX_BATCH > 100 {
		fast = fastTick.C
	}

	go r.stopAdapting()

	proposalSwitch := r.proposals

	for {

		select {

		case commit := <-r.commits:
			actionLog("<<COMMIT", r.id)
			r.commit(commit)

		case commitShort := <-r.commitsShort:
			actionLog("<<COMMITSHORT", r.id)
			r.commitShort(commitShort)

		case preAcceptanceOk := <-r.preAcceptanceOks:
			actionLog("<<PREACCEPTOK", r.id)
			r.preAcceptOK(preAcceptanceOk)

		case preAcceptanceReply := <-r.preAcceptanceReplies:
			actionLog("<<PREACCEPTREPLY", r.id)
			r.preAcceptReply(preAcceptanceReply)

		case preAcceptance := <-r.preAcceptances:
			actionLog("<<PREACCEPT", r.id)
			r.preAccept(preAcceptance)

		case acceptanceReply := <-r.acceptanceReplies:
			actionLog("<<ACCEPTREPLY", r.id)
			r.acceptReply(acceptanceReply)

		case tryPreAcceptance := <-r.tryPreAcceptances:
			actionLog("<<TRYPREACCEPT", r.id)
			r.tryPreAccept(tryPreAcceptance)

		case preparation := <-r.preparations:
			actionLog("<<PREPARE", r.id)
			r.prepare(preparation)

		case acceptance := <-r.acceptances:
			actionLog("<<ACCEPT", r.id)
			r.accept(acceptance)

		case preparationReply := <-r.preparationReplies:
			actionLog("<<PREPAREREPLY", r.id)
			r.prepareReply(preparationReply)

		case tryPreAcceptanceReply := <-r.tryPreAcceptanceReplies:
			actionLog("<<TRYPREACCEPTREPLY", r.id)
			r.tryPreAcceptReply(tryPreAcceptanceReply)

		case beacon := <-r.beacons:
			go r.cluster.ReplyPing(beacon.Replica, &BeaconReply{beacon.Timestamp, r.id})

		case <-slow:
			actionLog("PING", r.id)
			go r.cluster.Ping(&Beacon{r.id, 0.0})

		case iid := <-r.recoveryInstances:
			actionLog("RECOVERY", r.id)
			r.startRecoveryForInstance(iid.replica, iid.instance)
		case <-r.shutdown:
			log.Info("stop")
			return

		case proposal := <-proposalSwitch:
			actionLog("<<PROPOSE", r.id)
			r.propose(proposal)
			proposalSwitch = nil

		case <-fast:
			proposalSwitch = r.proposals
		}
	}
}

func buildPipeline() {

}

func actionLog(action string, replica int32) {
	log.WithFields(log.Fields{
		"replica": replica,
	}).Info(action)
}
