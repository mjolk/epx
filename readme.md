# Project Name : EPX 

pratical implementation of http://efficient.github.io/epaxos/
instead of integer keys and values it uses strings as keys/values
networking with grpc/google protocol buffers
- http://www.grpc.io
- https://developers.google.com/protocol-buffers/

it basically works, still trying to figure out some aspects 
of epaxos as proposed.

the algorithm is exaclty the same as the original (i hope, not established yet)
, just refactored a little
and changed the whole networking layer
it probably runs slower than the code from the research paper
and probably has more bugs but it's a start

## Installation

project created with go1.5.1

1. go get
2. cd epx-replica
3. go install
4. cd ..
5. cd epx-client
6. go install
7. cd ..
8. ./start.sh
** the start script assumes your $GOPATH/bin is on PATH
!!!! WAIT 10 secs (you can track with tail -f on one of the replica logs) before you start the epx-client
!!!! the replicas take 10 secs to organize the optimal paths they will use to contact each other
!!!! for the first round ( they update themselves while they are running but if you do not have the paths set
!!!! on the first run you might see some strange results, i haven't rigged the client yet to trigger multiple rounds form cli)
9. ctrl Z
10. bg
11. epx-client (again assumes $GOPATH/bin is on your PATH)
12. next you can track what's happening in logclient.log, logreplica_0...x.log
the code is still loaded with logging statements -> wip

##Todo

- further testing and verify i didn't break anything regarding correctness
- fill in some Todo's, less likely (or optimal regarding the algorithm) paths in the code still need some work
- write more tests to run and verify performance/correctness
- optimize

## Usage

lots of applications, but for now just for playing around with

## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## History

source : https://github.com/efficient/epaxos

## Credits

see source
