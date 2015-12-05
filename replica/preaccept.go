package replica

/**********************************************************************

                            PHASE 1

***********************************************************************/
import (
	log "github.com/Sirupsen/logrus"
)

func (r *epaxosReplica) preAccept(preAccept *PreAcceptance) {
	replicaCnt := r.cluster.Len()
	inst := r.instanceSpace[preAccept.Leader][preAccept.Instance]

	if preAccept.Seq >= r.maxSeq {
		r.maxSeq = preAccept.Seq + 1
	}

	if inst != nil && (inst.status == Status_COMMITTED || inst.status == Status_ACCEPTED) {
		//reordered handling of commit/accept and pre-accept
		if inst.commands == nil {
			r.instanceSpace[preAccept.Leader][preAccept.Instance].commands = preAccept.Commands
			r.updateConflicts(preAccept.Commands, preAccept.Replica,
				preAccept.Instance, preAccept.Seq)
			//r.instanceSpace[preAccept.Leader][preAccept.Instance].bfilter = bfFromCommands(preAccept.Command)
		}
		//	r.recordCommands(preAccept.Command)
		//	r.sync()
		return
	}

	if preAccept.Instance >= r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance + 1
	}

	//update attributes for command
	seq, deps, changed := r.updateAttributes(preAccept.Commands, preAccept.Seq, preAccept.Deps, preAccept.Replica)
	uncommittedDeps := false
	for q := 0; q < replicaCnt; q++ {
		if deps[q] > r.committedUpTo[q] {
			uncommittedDeps = true
			break
		}
	}
	status := Status_PREACCEPTED_EQ
	if changed {
		status = Status_PREACCEPTED
	}

	log.WithFields(log.Fields{
		"status": status,
	}).Info("STATUS PREACCEPT")

	if inst != nil {
		if preAccept.Ballot < inst.ballot {
			log.Info("REPLYPREACCEPT")
			go r.cluster.ReplyPreAccept(preAccept.Leader,
				&PreAcceptanceReply{
					preAccept.Replica,
					preAccept.Instance,
					false,
					inst.ballot,
					inst.seq,
					inst.deps,
					r.committedUpTo})
			return
		} else {
			inst.commands = preAccept.Commands
			inst.seq = seq
			inst.deps = deps
			inst.ballot = preAccept.Ballot
			inst.status = status
		}
	} else {
		r.instanceSpace[preAccept.Replica][preAccept.Instance] = &Instance{
			preAccept.Commands,
			preAccept.Ballot,
			status,
			seq,
			deps,
			nil, 0, 0,
			nil}
	}

	r.updateConflicts(preAccept.Commands, preAccept.Replica, preAccept.Instance, preAccept.Seq)

	//r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
	//r.recordCommands(preAccept.Command)
	//r.sync()

	if len(preAccept.Commands) == 0 {
		//checkpoint
		//update latest checkpoint info
		r.latestCPReplica = preAccept.Replica
		r.latestCPInstance = preAccept.Instance

		//discard dependency hashtables
		r.clearHashtables()
	}

	if changed || uncommittedDeps || preAccept.Replica != preAccept.Leader || !IsInitialBallot(preAccept.Ballot) {
		log.Info("replypreaccept")
		go r.cluster.ReplyPreAccept(preAccept.Leader,
			&PreAcceptanceReply{
				preAccept.Replica,
				preAccept.Instance,
				true,
				preAccept.Ballot,
				seq,
				deps,
				r.committedUpTo})
	} else {
		log.Info("precceptanceOK")
		go r.cluster.PreAcceptanceOk(preAccept.Leader, &PreAcceptanceOk{preAccept.Instance})
	}
}

func (r *epaxosReplica) preAcceptReply(pareply *PreAcceptanceReply) {
	cLen := r.cluster.Len()
	inst := r.instanceSpace[pareply.Replica][pareply.Instance]

	if inst.status != Status_PREACCEPTED {
		// we've moved on, this is a delayed reply
		log.Info("Delayed reply RETURN")
		return
	}

	if inst.ballot != pareply.Ballot {
		log.Info("RETURN")
		return
	}

	if !pareply.Ok {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if pareply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = pareply.Ballot
		}
		if inst.lb.nacks >= cLen/2 {
			// TODO
		}
		log.Info("RETURN")
		return
	}

	inst.lb.preAcceptOKs++

	log.Info("mergeattributes*****")
	var equal bool
	inst.seq, inst.deps, equal = r.mergeAttributes(inst.seq, inst.deps, pareply.Seq, pareply.Deps)
	if (cLen <= 3 && !r.thrifty) || inst.lb.preAcceptOKs > 1 {
		inst.lb.allEqual = inst.lb.allEqual && equal
		if !equal {
			conflicted++
		}
	}

	allCommitted := true
	for q := 0; q < cLen; q++ {
		if inst.lb.committedDeps[q] < pareply.CommittedDeps[q] {
			inst.lb.committedDeps[q] = pareply.CommittedDeps[q]
		}
		if inst.lb.committedDeps[q] < r.committedUpTo[q] {
			inst.lb.committedDeps[q] = r.committedUpTo[q]
		}
		if inst.lb.committedDeps[q] < inst.deps[q] {
			allCommitted = false
		}
	}

	//can we commit on the fast path?
	if inst.lb.preAcceptOKs >= cLen/2 && inst.lb.allEqual && allCommitted && IsInitialBallot(inst.ballot) {
		happy++
		r.instanceSpace[pareply.Replica][pareply.Instance].status = Status_COMMITTED
		r.updateCommitted(pareply.Replica)
		if inst.lb.clientProposals != nil && !r.dreply {
			// give clients the all clear
			cnt := len(inst.lb.clientProposals)
			log.WithFields(log.Fields{
				"proposals": cnt,
			}).Info("Sending poposal reply")
			for i := 0; i < cnt; i++ {
				c := i
				go r.cluster.ReplyProposeTS(
					&ProposalReplyTS{
						true,
						inst.lb.clientProposals[c].CommandId,
						NIL,
						inst.lb.clientProposals[c].Timestamp})
			}
		}

		//	r.recordInstanceMetadata(inst)
		//	r.sync() //is this necessary here?

		log.WithFields(log.Fields{
			"replica": r.id,
		}).Info("bcastcommit")
		r.bcastCommit(pareply.Replica, pareply.Instance, inst.commands, inst.seq, inst.deps)
	} else if inst.lb.preAcceptOKs >= cLen/2 {
		if !allCommitted {
			weird++
		}
		slow++
		inst.status = Status_ACCEPTED
		r.bcastAccept(pareply.Replica, pareply.Instance, inst.ballot, int32(len(inst.commands)), inst.seq, inst.deps)
	}
	log.Info("SLOW")
	//TODO: take the slow path if messages are slow to arrive
}

func (r *epaxosReplica) preAcceptOK(pareply *PreAcceptanceOk) {
	inst := r.instanceSpace[r.id][pareply.Instance]

	if inst.status != Status_PREACCEPTED {
		// we've moved on, this is a delayed reply
		log.Info("RETURN")
		return
	}

	if !IsInitialBallot(inst.ballot) {
		log.Info("RETURN")
		return
	}

	inst.lb.preAcceptOKs++
	cLen := r.cluster.Len()

	allCommitted := true
	for q := 0; q < cLen; q++ {
		if inst.lb.committedDeps[q] < inst.lb.originalDeps[q] {
			inst.lb.committedDeps[q] = inst.lb.originalDeps[q]
		}
		if inst.lb.committedDeps[q] < r.committedUpTo[q] {
			inst.lb.committedDeps[q] = r.committedUpTo[q]
		}
		if inst.lb.committedDeps[q] < inst.deps[q] {
			allCommitted = false
		}
	}

	//can we commit on the fast path?
	if inst.lb.preAcceptOKs >= cLen/2 && inst.lb.allEqual && allCommitted && IsInitialBallot(inst.ballot) {
		happy++
		r.instanceSpace[r.id][pareply.Instance].status = Status_COMMITTED
		r.updateCommitted(r.id)
		if inst.lb.clientProposals != nil && !r.dreply {
			// give clients the all clear
			pLen := len(inst.lb.clientProposals)
			log.WithFields(log.Fields{
				"proposals": pLen,
			}).Info("Sending poposal reply")
			for i := 0; i < pLen; i++ {
				c := i
				go r.cluster.ReplyProposeTS(
					&ProposalReplyTS{
						true,
						inst.lb.clientProposals[c].CommandId,
						NIL,
						inst.lb.clientProposals[c].Timestamp})
			}
		}

		//		r.recordInstanceMetadata(inst)
		//		r.sync() //is this necessary here?

		log.WithFields(log.Fields{
			"replica": r.id,
		}).Info("bcastcommit")
		r.bcastCommit(r.id, pareply.Instance, inst.commands, inst.seq, inst.deps)
	} else if inst.lb.preAcceptOKs >= cLen/2 {
		if !allCommitted {
			weird++
		}
		slow++
		inst.status = Status_ACCEPTED
		log.Info("bcastaccept")
		r.bcastAccept(r.id, pareply.Instance, inst.ballot, int32(len(inst.commands)), inst.seq, inst.deps)
	}
	log.Info("SLOW")
	//TODO: take the slow path if messages are slow to arrive
}
