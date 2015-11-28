package replica

/**********************************************************************

                        PHASE 2

***********************************************************************/

func (r *epaxosReplica) accept(accept *Acceptance) {
	inst := r.instanceSpace[accept.Leader][accept.Instance]

	if accept.Seq >= r.maxSeq {
		r.maxSeq = accept.Seq + 1
	}

	if inst != nil && (inst.status == Status_COMMITTED || inst.status == Status_EXECUTED) {
		return
	}

	if accept.Instance >= r.crtInstance[accept.Leader] {
		r.crtInstance[accept.Leader] = accept.Instance + 1
	}

	if inst != nil {
		if accept.Ballot < inst.ballot {
			r.cluster.ReplyAccept(accept.Leader, &AcceptanceReply{accept.Replica, accept.Instance, false, inst.ballot})
			return
		}
		inst.status = Status_ACCEPTED
		inst.seq = accept.Seq
		inst.deps = accept.Deps
	} else {
		r.instanceSpace[accept.Leader][accept.Instance] = &Instance{
			nil,
			accept.Ballot,
			Status_ACCEPTED,
			accept.Seq,
			accept.Deps,
			nil, 0, 0, nil}

		if accept.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = accept.Replica
			r.latestCPInstance = accept.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}

	//	r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
	//	r.sync()

	r.cluster.ReplyAccept(accept.Leader,
		&AcceptanceReply{
			accept.Replica,
			accept.Instance,
			false,
			accept.Ballot})
}

func (r *epaxosReplica) acceptReply(areply *AcceptanceReply) {
	inst := r.instanceSpace[areply.Replica][areply.Instance]
	cLen := r.cluster.Len()

	if inst.status != Status_ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if inst.ballot != areply.Ballot {
		return
	}

	if !areply.Ok {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= cLen/2 {
			// TODO
		}
		return
	}

	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > cLen/2 {
		r.instanceSpace[areply.Replica][areply.Instance].status = Status_COMMITTED
		r.updateCommitted(areply.Replica)
		if inst.lb.clientProposals != nil && !r.dreply {
			// give clients the all clear
			pLen := len(inst.lb.clientProposals)
			for i := 0; i < pLen; i++ {
				r.cluster.ReplyProposeTS(
					&ProposalReplyTS{
						false,
						inst.lb.clientProposals[i].CommandId,
						NIL,
						inst.lb.clientProposals[i].Timestamp,
						inst.lb.clientProposals[i].Client})
			}
		}

		//		r.recordInstanceMetadata(inst)
		//		r.sync() //is this necessary here?

		r.bcastCommit(areply.Replica, areply.Instance, inst.commands, inst.seq, inst.deps)
	}
}
