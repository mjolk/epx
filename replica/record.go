package replica

func (r *epaxosReplica) recordInstanceMetadata(inst *Instance) {
	if !r.durable {
		return
	}
	//TODO write to stabke store
}

//write a sequence of commands to stable storage
func (r *epaxosReplica) recordCommands(cmds []Command) {
	if !r.durable {
		return
	}

	if cmds == nil {
		return
	}

	//TODO write to stable storage
}

//sync with the stable store
func (r *epaxosReplica) sync() {
	if !r.durable {
		return
	}
	//TODO write to stable store
}
