bismark-passive-server-go
=========================

A package for analyzing bismark-passive data in Go.

Installation
------------

1. Install Go. See http://golang.org. Make sure you set $GOPATH (probably to $HOME/go).
2. Install leveldb. See http://code.google.com/p/leveldb. If you're running Debian, the libleveldb-dev package is probably what you want.
3. `go get github.com/sburnett/bismark-passive-server-go`
4. The binary is in $GOPATH/bin/bismark-passive-server-go

Installation without root
-------------------------

1. `pip install mercurial`
2. Make sure `.local/bin` is in your $PATH.
3. Build Go from source. See http://golang.org/doc/install/source. Once you've extracted and compiled Go with `all.bash` you shouldn't move the installation elsewhere, so make sure you perform the build in a suitable non-NFS and non-tmp directory.
4. Be sure to set your $GOPATH according to the Go documentation.
5. Make sure `$YOUR_GO_INSTALLATION/bin` is in your $PATH.
6. Download leveldb from http://code.google.com/p/leveldb. Build it in a suitable non-tmp directory. `export LEVELDB_PATH="/wherever/you/built/leveldb"`
7. `LD_LIBRARY_PATH="" CGO_CFLAGS="-I$LEVELDB_PATH/include" CGO_LDFLAGS="-L$LEVELDB_PATH" go get -x -u github.com/jmhodges/levigo`
8. `go get github.com/sburnett/bismark-passive-server-go`

[![Build Status](https://travis-ci.org/sburnett/bismark-passive-server-go.png)](https://travis-ci.org/sburnett/bismark-passive-server-go)
