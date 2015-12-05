package replica

/**********************************************************************

                            PHASE 1

***********************************************************************/
import (
	log "github.com/Sirupsen/logrus"
)

func (r *epaxosReplica) propose(proposal *Proposal) {

	batchSize := len(r.proposals)
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	} else if batchSize == 0 {
		batchSize = 1
	}

	instNo := r.crtInstance[r.id]
	r.crtInstance[r.id]++

	cmds := make([]*Command, batchSize)
	proposals := make([]*Proposal, batchSize)
	cmds[0] = proposal.Command
	proposals[0] = proposal
	for i := 1; i < batchSize; i++ {
		prop := <-r.proposals
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	r.startPhase1(r.id, instNo, 0, proposals, cmds, batchSize)

}

func (r *epaxosReplica) startPhase1(replica int32, instance int32, ballot int32,
	proposals []*Proposal, cmds []*Command, batchSize int) {
	//init command attributes
	log.Info("PHASE 1")

	seq := int32(0)
	deps := []int32{-1, -1, -1, -1, -1}

	seq, deps, _ = r.updateAttributes(cmds, seq, deps, replica)

	r.instanceSpace[r.id][instance] = &Instance{
		cmds,
		ballot,
		Status_PREACCEPTED,
		seq,
		deps,
		&LeaderBookkeeping{proposals, 0, 0, true, 0, 0, 0, deps,
			[]int32{-1, -1, -1, -1, -1}, nil, false, false, nil, 0}, 0, 0,
		nil}

	r.updateConflicts(cmds, r.id, instance, seq)

	if seq >= r.maxSeq {
		r.maxSeq = seq + 1
	}

	//r.recordInstanceMetadata(r.InstanceSpace[r.id][instance])
	//r.recordCommands(cmds)
	//r.sync()

	log.WithFields(log.Fields{
		"replica":  r.id,
		"instance": instance,
		"ballot":   ballot,
		"cmds":     len(cmds),
		"sequence": seq,
		"deps":     deps,
	}).Info("BCASTPREACCEPT>>")

	r.bcastPreAccept(r.id, instance, ballot, cmds, seq, deps)

	cpcounter += batchSize

	if r.id == 0 && DO_CHECKPOINTING && cpcounter >= CHECKPOINT_PERIOD {
		cpcounter = 0

		//Propose a checkpoint command to act like a barrier.
		//This allows replicas to discard their dependency hashtables.
		r.crtInstance[r.id]++
		instance++

		r.maxSeq++
		cLen := r.cluster.Len()
		for q := 0; q < cLen; q++ {
			deps[q] = r.crtInstance[q] - 1
		}

		r.instanceSpace[r.id][instance] = &Instance{
			cpMarker,
			0,
			Status_PREACCEPTED,
			r.maxSeq,
			deps,
			&LeaderBookkeeping{nil, 0, 0, true, 0, 0, 0, deps,
				nil, nil, false, false, nil, 0},
			0,
			0,
			nil}

		r.latestCPReplica = r.id
		r.latestCPInstance = instance

		//discard dependency hashtables
		r.clearHashtables()

		//r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
		//r.sync()

		r.bcastPreAccept(r.id, instance, 0, cpMarker, r.maxSeq, deps)
	}
}
