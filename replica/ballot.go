package replica

func UniqueBallot(r Replica, ballot int32) int32 {
	return (ballot << 4) | r.Id()
}

func BallotLargerThan(r Replica, ballot int32) int32 {
	return UniqueBallot(r, (ballot>>4)+1)
}

func ReplicaFromBallot(ballot int32) int32 {
	return ballot & 15
}

func IsInitialBallot(ballot int32) bool {
	return (ballot >> 4) == 0
}

func (r *epaxosReplica) bcastCommit(replica int32, instance int32,
	cmds []*Command, seq int32, deps []int32) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	ec := new(TryCommit)
	ec.Leader = r.id
	ec.Replica = replica
	ec.Instance = instance
	ec.Commands = cmds
	ec.Seq = seq
	ec.Deps = deps
	ecs := new(TryCommitShort)
	ecs.Leader = r.id
	ecs.Replica = replica
	ecs.Instance = instance
	ecs.Count = int32(len(cmds))
	ecs.Seq = seq
	ecs.Deps = deps

	r.cluster.Commit(r.thrifty, ec, ecs)
}

func (r *epaxosReplica) bcastAccept(replica int32, instance int32, ballot int32,
	count int32, seq int32, deps []int32) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	ea := new(Acceptance)
	ea.Leader = r.id
	ea.Replica = replica
	ea.Instance = instance
	ea.Ballot = ballot
	ea.Count = count
	ea.Seq = seq
	ea.Deps = deps

	r.cluster.Accept(r.thrifty, ea)

}

func (r *epaxosReplica) bcastPreAccept(replica int32, instance int32, ballot int32,
	cmds []*Command, seq int32, deps []int32) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	pa := new(PreAcceptance)
	pa.Leader = r.id
	pa.Replica = replica
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Commands = cmds
	pa.Seq = seq
	pa.Deps = deps
	r.cluster.PreAccept(r.thrifty, pa)
}

func (r *epaxosReplica) bcastPrepare(replica int32, instance int32, ballot int32) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	preparation := &Preparation{r.id, replica, instance, ballot}
	r.cluster.Prepare(r.thrifty, r.id, preparation)
}

var tpa *TryPreAcceptance

func (r *epaxosReplica) bcastTryPreAccept(replica int32, instance int32, ballot int32,
	cmds []*Command, seq int32, deps []int32) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	tpa.Leader = r.id
	tpa.Replica = replica
	tpa.Instance = instance
	tpa.Ballot = ballot
	tpa.Commands = cmds
	tpa.Seq = seq
	tpa.Deps = deps

	r.cluster.TryPreAccept(r.id, tpa)
}
