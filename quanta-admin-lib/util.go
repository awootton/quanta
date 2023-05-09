package admin

import (
	"fmt"
	"log"

	"github.com/disney/quanta/shared"
	"github.com/hashicorp/consul/api"
)

func GetClientConnection(consulAddr string, port int) *shared.Conn {

	fmt.Printf("Connecting to Consul at: [%s] ...\n", consulAddr)
	consulClient, err := api.NewClient(&api.Config{Address: consulAddr})
	if err != nil {
		fmt.Println("Is the consul agent running?")
		log.Fatal(err)
	}
	fmt.Printf("Connecting to Quanta services at port: [%d] ...\n", port)
	conn := shared.NewDefaultConnection()
	conn.ServiceName = "quanta" // "quanta-node"
	conn.ServicePort = port
	conn.Quorum = 0
	if err := conn.Connect(consulClient); err != nil {
		log.Fatal(err)
	}
	return conn
}
