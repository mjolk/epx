# Project Name : EPX 

pratical implementation of http://efficient.github.io/epaxos/
instead of integer keys and values it uses strings as keys/values
networking with grpc/google protocol buffers
http://www.grpc.io
https://developers.google.com/protocol-buffers/

it basically works, still trying to figure out some aspects 
of epaxos as proposed.

the algorithm is exaclty the same as the original (i hope, not established yet)
, just refactored a little
and changed the whole networking layer
it probably runs slower than the code from the research paper
and probably has more bugs but it's a start

## Installation

go get
cd epx-replica
go install
cd ..
cd epx-client
go install
cd ..
./start.sh
** the start script assumes your $GOPATH/bin is on PATH
ctrl Z
bg
epx-client (again assumes $GOPATH/bin is on your PATH)
then you can track what's happening in logclient.log, logreplica_0...x.log
the code is still loaded with logging statements -> wip

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
