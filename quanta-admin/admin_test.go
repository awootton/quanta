package main

import (
	"testing"

	admin "github.com/disney/quanta/quanta-admin-lib"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
)

// TestCreateCmd requires a cluster to already be running
func TestCreateCmd(t *testing.T) {

	create := admin.CreateCmd{
		Table:     "cities",
		SchemaDir: "../configuration",
		Confirm:   true,
	}
	create.Run(&proxy.Context{ConsulAddr: "127.0.0.1:8500", Port: 8500, Debug: true})
}
