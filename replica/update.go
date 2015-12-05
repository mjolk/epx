package replica

func (r *epaxosReplica) updateConflicts(cmds []*Command, replica int32, instance int32, seq int32) {
	for _, cmd := range cmds {
		if d, present := r.conflicts[replica][cmd.Key]; present {
			if d < instance {
				r.conflicts[replica][cmd.Key] = instance
			}
		} else {
			r.conflicts[replica][cmd.Key] = instance
		}
		if s, present := r.maxSeqPerKey[cmd.Key]; present {
			if s < seq {
				r.maxSeqPerKey[cmd.Key] = seq
			}
		} else {
			r.maxSeqPerKey[cmd.Key] = seq
		}
	}
}

func (r *epaxosReplica) updateAttributes(cmds []*Command, seq int32, deps []int32,
	replica int32) (int32, []int32, bool) {
	changed := false
	replicaCnt := r.cluster.Len()
	for q := 0; q < replicaCnt; q++ {
		if r.Id() != replica && int32(q) == replica {
			continue
		}
		for _, cmd := range cmds {
			if d, present := r.conflicts[q][cmd.Key]; present {
				if d > deps[q] {
					deps[q] = d
					if seq <= r.instanceSpace[q][d].seq {
						seq = r.instanceSpace[q][d].seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for _, cmd := range cmds {
		if s, present := r.maxSeqPerKey[cmd.Key]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}

	return seq, deps, changed
}

func (r *epaxosReplica) mergeAttributes(seq1 int32, deps1 []int32, seq2 int32,
	deps2 []int32) (int32, []int32, bool) {
	cLen := r.cluster.Len()
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for q := 0; q < cLen; q++ {
		if int32(q) == r.id {
			continue
		}
		if deps1[q] != deps2[q] {
			equal = false
			if deps2[q] > deps1[q] {
				deps1[q] = deps2[q]
			}
		}
	}
	return seq1, deps1, equal
}

func (r *epaxosReplica) updateCommitted(replica int32) {
	for r.instanceSpace[replica][r.committedUpTo[replica]+1] != nil &&
		(r.instanceSpace[replica][r.committedUpTo[replica]+1].status == Status_COMMITTED ||
			r.instanceSpace[replica][r.committedUpTo[replica]+1].status == Status_EXECUTED) {
		r.committedUpTo[replica] = r.committedUpTo[replica] + 1
	}
}

func (r *epaxosReplica) clearHashtables() {
	cLen := r.cluster.Len()
	for q := 0; q < cLen; q++ {
		r.conflicts[q] = make(map[string]int32, HT_INIT_SIZE)
	}
}
