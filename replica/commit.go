package replica

/**********************************************************************

                            COMMIT

***********************************************************************/

func (r *epaxosReplica) commit(commit *TryCommit) {
	inst := r.instanceSpace[commit.Replica][commit.Instance]

	if commit.Seq >= r.maxSeq {
		r.maxSeq = commit.Seq + 1
	}

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil && len(commit.Commands) == 0 {
			//someone committed a NO-OP, but we have proposals for this instance
			//try in a different instance
			for _, p := range inst.lb.clientProposals {
				r.proposals <- p
			}
			inst.lb = nil
		}
		inst.seq = commit.Seq
		inst.deps = commit.Deps
		inst.status = Status_COMMITTED
	} else {
		r.instanceSpace[commit.Replica][int(commit.Instance)] = &Instance{
			commit.Commands,
			0,
			Status_COMMITTED,
			commit.Seq,
			commit.Deps,
			nil,
			0,
			0,
			nil}
		r.updateConflicts(commit.Commands, commit.Replica, commit.Instance, commit.Seq)

		if len(commit.Commands) == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)

	//	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	//	r.recordCommands(commit.Command)
}

func (r *epaxosReplica) commitShort(commit *TryCommitShort) {
	inst := r.instanceSpace[commit.Replica][commit.Instance]

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//try command in a different instance
			for _, p := range inst.lb.clientProposals {
				r.proposals <- p
			}
			inst.lb = nil
		}
		inst.seq = commit.Seq
		inst.deps = commit.Deps
		inst.status = Status_COMMITTED
	} else {
		r.instanceSpace[commit.Replica][commit.Instance] = &Instance{
			nil,
			0,
			Status_COMMITTED,
			commit.Seq,
			commit.Deps,
			nil, 0, 0, nil}

		if commit.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)

	//	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
}
