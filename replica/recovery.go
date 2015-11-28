package replica

/**********************************************************************

                      RECOVERY ACTIONS

***********************************************************************/

func equal(deps1 []int32, deps2 []int32) bool {
	deps1Len := len(deps1)
	for i := 0; i < deps1Len; i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

var deferMap map[uint64]uint64 = make(map[uint64]uint64)

func updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}

func ConflictBatch(batch1 []*Command, batch2 []*Command) bool {
	batchLen := len(batch1)
	for i := 0; i < batchLen; i++ {
		for j := 0; j < len(batch2); j++ {
			if conflict(batch1[i], batch2[j]) {
				return true
			}
		}
	}
	return false
}

func (r *epaxosReplica) startRecoveryForInstance(replica int32, instance int32) {
	var nildeps []int32

	if r.instanceSpace[replica][instance] == nil {
		r.instanceSpace[replica][instance] = &Instance{nil, 0, Status_NONE, 0, nildeps, nil, 0, 0, nil}
	}

	inst := r.instanceSpace[replica][instance]
	if inst.lb == nil {
		inst.lb = &LeaderBookkeeping{nil, -1, 0, false, 0, 0, 0, nildeps, nil, nil, true, false, nil, 0}

	} else {
		inst.lb = &LeaderBookkeeping{inst.lb.clientProposals, -1, 0, false, 0, 0, 0, nildeps, nil, nil, true, false, nil, 0}
	}

	if inst.status == Status_ACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.commands, inst.status, inst.seq, inst.deps, 0, false}
		inst.lb.maxRecvBallot = inst.ballot
	} else if inst.status >= Status_PREACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.commands, inst.status, inst.seq, inst.deps, 1, (r.id == replica)}
	}

	//compute larger ballot
	inst.ballot = BallotLargerThan(r, inst.ballot)

	r.bcastPrepare(replica, instance, inst.ballot)
}

func (r *epaxosReplica) findPreAcceptConflicts(cmds []*Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	inst := r.instanceSpace[replica][instance]
	commandLen := len(inst.commands)
	cLen := r.cluster.Len()
	if inst != nil && commandLen > 0 {
		if inst.status >= Status_ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		if inst.seq == tpa.Seq && equal(inst.deps, tpa.Deps) {
			// already PRE-ACCEPTED, no point looking for conflicts again
			return false, replica, instance
		}
	}
	for q := int32(0); q < int32(cLen); q++ {
		for i := r.executedUpTo[q]; i < r.crtInstance[q]; i++ {
			if replica == q && instance == i {
				// no point checking past instance in replica's row, since replica would have
				// set the dependencies correctly for anything started after instance
				break
			}
			if i == deps[q] {
				//the instance cannot be a dependency for itself
				continue
			}
			inst := r.instanceSpace[q][i]
			if inst == nil || inst.commands == nil || len(inst.commands) == 0 {
				continue
			}
			if inst.deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if ConflictBatch(inst.commands, cmds) {
				if i > deps[q] ||
					(i < deps[q] && inst.seq >= seq && (q != replica || inst.status > Status_PREACCEPTED_EQ)) {
					// this is a conflict
					return true, q, i
				}
			}
		}
	}
	return false, -1, -1
}

func (r *epaxosReplica) prepare(prepare *Preparation) {
	inst := r.instanceSpace[prepare.Replica][prepare.Instance]
	var preply *PreparationReply
	var nildeps []int32

	if inst == nil {
		r.instanceSpace[prepare.Replica][prepare.Instance] = &Instance{
			nil,
			prepare.Ballot,
			Status_NONE,
			0,
			nildeps,
			nil, 0, 0, nil}
		preply = &PreparationReply{
			r.id,
			prepare.Replica,
			prepare.Instance,
			true,
			-1,
			Status_NONE,
			nil,
			-1,
			nildeps}
	} else {
		ok := true
		if prepare.Ballot < inst.ballot {
			ok = false
		} else {
			inst.ballot = prepare.Ballot
		}
		preply = &PreparationReply{
			r.id,
			prepare.Replica,
			prepare.Instance,
			ok,
			inst.ballot,
			inst.status,
			inst.commands,
			inst.seq,
			inst.deps}
	}

	r.cluster.ReplyPrepare(prepare.Leader, preply)
}

func (r *epaxosReplica) prepareReply(preply *PreparationReply) {
	inst := r.instanceSpace[preply.Replica][preply.Instance]

	if inst.lb == nil || !inst.lb.preparing {
		// we've moved on -- these are delayed replies, so just ignore
		// TODO: should replies for non-current ballots be ignored?
		return
	}

	if !preply.Ok {
		// TODO: there is probably another active leader, back off and retry later
		inst.lb.nacks++
		return
	}

	//Got an ACK (preply.OK == TRUE)

	inst.lb.prepareOKs++

	if preply.Status == Status_COMMITTED || preply.Status == Status_EXECUTED {
		r.instanceSpace[preply.Replica][preply.Instance] = &Instance{
			preply.Commands,
			inst.ballot,
			Status_COMMITTED,
			preply.Seq,
			preply.Deps,
			nil, 0, 0, nil}
		r.bcastCommit(preply.Replica, preply.Instance, inst.commands, preply.Seq, preply.Deps)
		//TODO: check if we should send notifications to clients
		return
	}

	if preply.Status == Status_ACCEPTED {
		if inst.lb.recoveryInst == nil || inst.lb.maxRecvBallot < preply.Ballot {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Commands, preply.Status, preply.Seq, preply.Deps, 0, false}
			inst.lb.maxRecvBallot = preply.Ballot
		}
	}

	if (preply.Status == Status_PREACCEPTED || preply.Status == Status_PREACCEPTED_EQ) &&
		(inst.lb.recoveryInst == nil || inst.lb.recoveryInst.status < Status_ACCEPTED) {
		if inst.lb.recoveryInst == nil {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Commands, preply.Status, preply.Seq, preply.Deps, 1, false}
		} else if preply.Seq == inst.seq && equal(preply.Deps, inst.deps) {
			inst.lb.recoveryInst.preAcceptCount++
		} else if preply.Status == Status_PREACCEPTED_EQ {
			// If we get different ordering attributes from pre-acceptors, we must go with the ones
			// that agreed with the initial command leader (in case we do not use Thrifty).
			// This is safe if we use thrifty, although we can also safely start phase 1 in that case.
			inst.lb.recoveryInst = &RecoveryInstance{preply.Commands, preply.Status, preply.Seq, preply.Deps, 1, false}
		}
		if preply.Acceptor == preply.Replica {
			//if the reply is from the initial command leader, then it's safe to restart phase 1
			inst.lb.recoveryInst.leaderResponded = true
			return
		}
	}

	cLen := r.cluster.Len()

	if inst.lb.prepareOKs < cLen/2 {
		return
	}

	//Received Prepare replies from a majority

	ir := inst.lb.recoveryInst

	if ir != nil {
		//at least one replica has (pre-)accepted this instance
		if ir.status == Status_ACCEPTED ||
			(!ir.leaderResponded && ir.preAcceptCount >= cLen/2 && (r.thrifty || ir.status == Status_PREACCEPTED_EQ)) {
			//safe to go to Accept phase
			inst.commands = ir.cmds
			inst.seq = ir.seq
			inst.deps = ir.deps
			inst.status = Status_ACCEPTED
			inst.lb.preparing = false
			r.bcastAccept(preply.Replica, preply.Instance, inst.ballot, int32(len(inst.commands)), inst.seq, inst.deps)
		} else if !ir.leaderResponded && ir.preAcceptCount >= (cLen/2+1)/2 {
			//send TryPreAccepts
			//but first try to pre-accept on the local replica
			inst.lb.preAcceptOKs = 0
			inst.lb.nacks = 0
			inst.lb.possibleQuorum = make([]bool, cLen)
			for q := 0; q < cLen; q++ {
				inst.lb.possibleQuorum[q] = true
			}
			if conf, q, i := r.findPreAcceptConflicts(ir.cmds, preply.Replica, preply.Instance, ir.seq, ir.deps); conf {
				if r.instanceSpace[q][i].status >= Status_COMMITTED {
					//start Phase1 in the initial leader's instance
					r.startPhase1(preply.Replica, preply.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
					return
				} else {
					inst.lb.nacks = 1
					inst.lb.possibleQuorum[r.id] = false
				}
			} else {
				inst.commands = ir.cmds
				inst.seq = ir.seq
				inst.deps = ir.deps
				inst.status = Status_PREACCEPTED
				inst.lb.preAcceptOKs = 1
			}
			inst.lb.preparing = false
			inst.lb.tryingToPreAccept = true
			r.bcastTryPreAccept(preply.Replica, preply.Instance, inst.ballot, inst.commands, inst.seq, inst.deps)
		} else {
			//start Phase1 in the initial leader's instance
			inst.lb.preparing = false
			r.startPhase1(preply.Replica, preply.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
		}
	} else {
		//try to finalize instance by proposing NO-OP
		var noop_deps []int32
		// commands that depended on this instance must look at all previous instances
		noop_deps[preply.Replica] = preply.Instance - 1
		inst.lb.preparing = false
		r.instanceSpace[preply.Replica][preply.Instance] = &Instance{
			nil,
			inst.ballot,
			Status_ACCEPTED,
			0,
			noop_deps,
			inst.lb, 0, 0, nil}
		r.bcastAccept(preply.Replica, preply.Instance, inst.ballot, 0, 0, noop_deps)
	}
}

func (r *epaxosReplica) tryPreAccept(tpa *TryPreAcceptance) {
	inst := r.instanceSpace[tpa.Replica][tpa.Instance]
	if inst != nil && inst.ballot > tpa.Ballot {
		// ballot number too small
		r.cluster.ReplyTryPreAccept(tpa.Leader, &TryPreAcceptanceReply{
			r.id,
			tpa.Replica,
			tpa.Instance,
			false,
			inst.ballot,
			tpa.Replica,
			tpa.Instance,
			inst.status})
	}
	if conflict, confRep, confInst := r.findPreAcceptConflicts(tpa.Commands, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
		// there is a conflict, can't pre-accept
		r.cluster.ReplyTryPreAccept(tpa.Leader, &TryPreAcceptanceReply{
			r.id,
			tpa.Replica,
			tpa.Instance,
			false,
			inst.ballot,
			confRep,
			confInst,
			r.instanceSpace[confRep][confInst].status})
	} else {
		// can pre-accept
		if tpa.Instance >= r.crtInstance[tpa.Replica] {
			r.crtInstance[tpa.Replica] = tpa.Instance + 1
		}
		if inst != nil {
			inst.commands = tpa.Commands
			inst.deps = tpa.Deps
			inst.seq = tpa.Seq
			inst.status = Status_PREACCEPTED
			inst.ballot = tpa.Ballot
		} else {
			r.instanceSpace[tpa.Replica][tpa.Instance] = &Instance{
				tpa.Commands,
				tpa.Ballot,
				Status_PREACCEPTED,
				tpa.Seq,
				tpa.Deps,
				nil, 0, 0,
				nil}
		}
		r.cluster.ReplyTryPreAccept(tpa.Leader, &TryPreAcceptanceReply{r.id, tpa.Replica, tpa.Instance, true, inst.ballot, 0, 0, 0})
	}
}

func (r *epaxosReplica) tryPreAcceptReply(tpar *TryPreAcceptanceReply) {
	inst := r.instanceSpace[tpar.Replica][tpar.Instance]
	if inst == nil || inst.lb == nil || !inst.lb.tryingToPreAccept || inst.lb.recoveryInst == nil {
		return
	}

	ir := inst.lb.recoveryInst
	cLen := r.cluster.Len()

	if tpar.Ok {
		inst.lb.preAcceptOKs++
		inst.lb.tpaOKs++
		if inst.lb.preAcceptOKs >= cLen/2 {
			//it's safe to start Accept phase
			inst.commands = ir.cmds
			inst.seq = ir.seq
			inst.deps = ir.deps
			inst.status = Status_ACCEPTED
			inst.lb.tryingToPreAccept = false
			inst.lb.acceptOKs = 0
			r.bcastAccept(tpar.Replica, tpar.Instance, inst.ballot, int32(len(inst.commands)), inst.seq, inst.deps)
			return
		}
	} else {
		inst.lb.nacks++
		if tpar.Ballot > inst.ballot {
			//TODO: retry with higher ballot
			return
		}
		inst.lb.tpaOKs++
		if tpar.ConflictReplica == tpar.Replica && tpar.ConflictInstance == tpar.Instance {
			//TODO: re-run prepare
			inst.lb.tryingToPreAccept = false
			return
		}
		inst.lb.possibleQuorum[tpar.Acceptor] = false
		inst.lb.possibleQuorum[tpar.ConflictReplica] = false
		notInQuorum := 0
		for q := 0; q < cLen; q++ {
			if !inst.lb.possibleQuorum[tpar.Acceptor] {
				notInQuorum++
			}
		}
		if tpar.ConflictStatus >= Status_COMMITTED || notInQuorum > cLen/2 {
			//abandon recovery, restart from phase 1
			inst.lb.tryingToPreAccept = false
			r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
		}
		if notInQuorum == cLen/2 {
			//this is to prevent defer cycles
			if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
				if inst.lb.possibleQuorum[dq] {
					//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
					//abandon recovery, restart from phase 1
					inst.lb.tryingToPreAccept = false
					r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
				}
			}
		}
		if inst.lb.tpaOKs >= cLen/2 {
			//defer recovery and update deferred information
			updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
			inst.lb.tryingToPreAccept = false
		}
	}
}
