// Package test - This code creates an in-memory "stack" and loads test data.
package test

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/disney/quanta/custom/functions"
	proxy "github.com/disney/quanta/quanta-proxy-lib"
	"github.com/disney/quanta/server"
	"github.com/disney/quanta/shared"
	"github.com/disney/quanta/sink"
	"github.com/disney/quanta/source"
	"github.com/hashicorp/consul/api"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/schema"
)

// Setup - Initialize test harness
func Setup() (*server.Node, error) {

	os.Mkdir("./testdata/bitmap", 0755)
	// Enable in memory instance
	node, err := server.NewNode("TEST", 0, "", "./testdata", "test", nil)
	if err != nil {
		return nil, err
	}
	kvStore := server.NewKVStore(node)
	node.AddNodeService(kvStore)
	search := server.NewStringSearch(node)
	node.AddNodeService(search)
	bitmapIndex := server.NewBitmapIndex(node, 0)
	node.AddNodeService(bitmapIndex)
	go func() {
		node.Start()
	}()
	err = node.InitServices()
	if err != nil {
		return nil, err
	}
	return node, nil
}

// RemoveContents - Remove local data files.
func RemoveContents(path string) error {
	files, err := filepath.Glob(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func StartNodes(nodeStart int) (*server.Node, error) {

	Version := "v0.0.1"
	Build := "2006-01-01"

	environment := "DEV"
	logLevel := "DEBUG"

	shared.InitLogging(logLevel, environment, "Data-Node", Version, "Quanta")

	index := nodeStart
	// for index = nodeStart; index < nodeStart+nodeCount; index++
	{
		hashKey := "quanta-node-" + strconv.Itoa(index)
		dataDir := "localClusterData/" + hashKey + "/data"
		bindAddr := "127.0.0.1"
		port := 4010 + index

		consul := bindAddr + ":8500"

		memLimit := 0

		// Create /bitmap data directory
		fmt.Printf("Creating bitmap data directory: %s", dataDir+"/bitmap")
		if _, err := os.Stat(dataDir + "/bitmap"); err != nil {
			err = os.MkdirAll(dataDir+"/bitmap", 0777)
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}

		// go func() { // if we run this it has to be just once
		// 	// Initialize Prometheus metrics endpoint.
		// 	http.Handle("/metrics", promhttp.Handler())
		// 	http.ListenAndServe(":2112", nil)
		// }()

		u.Infof("Node identifier '%s'", hashKey)

		u.Infof("Connecting to Consul at: [%s] ...\n", consul)
		consulClient, err := api.NewClient(&api.Config{Address: consul})
		if err != nil {
			u.Errorf("Is the consul agent running?")
			log.Fatalf("[node: Cannot initialize endpoint config: error: %s", err)
		}

		newNodeName := hashKey
		// newNodeName = "quanta"
		m, err := server.NewNode(fmt.Sprintf("%v:%v", Version, Build), int(port), bindAddr, dataDir, newNodeName, consulClient)
		if err != nil {
			u.Errorf("[node: Cannot initialize node config: error: %s", err)
		}
		m.ServiceName = "quanta" // not hashKey this doesn't work was "quanta-node" (atw)
		m.IsLocalCluster = true

		kvStore := server.NewKVStore(m)
		m.AddNodeService(kvStore)

		search := server.NewStringSearch(m)
		m.AddNodeService(search)

		bitmapIndex := server.NewBitmapIndex(m, int(memLimit))
		m.AddNodeService(bitmapIndex)

		// Start listening endpoint
		m.Start()

		start := time.Now()
		err = m.InitServices()
		elapsed := time.Since(start)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Node initialized in %v.", elapsed)

		go func() {
			joinName := "quanta"   // this is the name for a cluster of nodes was "quanta-node"
			err = m.Join(joinName) // this does not return
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
			fmt.Println("StartNodes returned from join")

			<-m.Stop
			select {
			case err = <-m.Err:
			default:
			}
			if err != nil {
				u.Errorf("[node: Cannot initialize endpoint config: error: %s", err)
			}
		}()
		return m, nil
	}
	// for {
	// 	time.Sleep(1 * time.Second)
	// }
}

type LocalProxyControl struct {
	Stop chan bool
}

func StartProxy(count int, testDataPath string) *LocalProxyControl {

	localProxy := &LocalProxyControl{}

	fmt.Println("Starting proxy")

	// for index := 0; index < count; index++ { // TODO: more than one proxy
	index := 0

	proxy.SetupCounters()
	proxy.Init()

	logging := "DEBUG"
	environment := "DEV"
	Version := "1.0.0"
	proxy.ConsulAddr = "127.0.0.1:8500"
	// cognito url for token service publicKeyURL := "" // unused
	proxy.QuantaPort = 4010
	proxyHostPort := 4000 + index

	// region := "us-east-1"

	if strings.ToUpper(logging) == "DEBUG" || strings.ToUpper(logging) == "TRACE" {
		if strings.ToUpper(logging) == "TRACE" {
			expr.Trace = true
		}
		u.SetupLogging("debug")
	} else {
		shared.InitLogging(logging, environment, "Proxy", Version, "Quanta")
	}

	// go func() { // FIXME: do this later
	// 	// Initialize Prometheus metrics endpoint.
	// 	http.Handle("/metrics", promhttp.Handler())
	// 	http.ListenAndServe(":2112", nil)
	// }()

	log.Printf("Connecting to Consul at: [%s] ...\n", proxy.ConsulAddr)
	consulConfig := &api.Config{Address: proxy.ConsulAddr}
	errx := shared.RegisterSchemaChangeListener(consulConfig, proxy.SchemaChangeListener)
	if errx != nil {
		u.Error(errx)
		os.Exit(1)
	}

	fmt.Println("Proxy RegisterSchemaChangeListener done")

	poolSize := 3

	// If the pool size is not configured then set it to the number of available CPUs
	// this is weird atw
	sessionPoolSize := poolSize
	if sessionPoolSize == 0 {
		sessionPoolSize = runtime.NumCPU()
		log.Printf("Session Pool Size not set, defaulting to number of available CPUs = %d", sessionPoolSize)
	} else {
		log.Printf("Session Pool Size = %d", sessionPoolSize)
	}

	// Match 2 or more whitespace chars inside string
	reWhitespace := regexp.MustCompile(`[\s\p{Zs}]{2,}`)
	_ = reWhitespace

	// load all of our built-in functions
	builtins.LoadAllBuiltins()
	sink.LoadAll()      // Register output sinks
	functions.LoadAll() // Custom functions

	// start cloud watch or prometheus metrics?

	fmt.Println("Proxy before NewQuantaSource")

	// Construct Quanta source
	tableCache := shared.NewTableCacheStruct() // is this right?

	// configDir := "../test/testdata" // gets: ../test/testdata/config/schema.yaml
	configDir := ""
	src, err := source.NewQuantaSource(tableCache, configDir, proxy.ConsulAddr, proxy.QuantaPort, sessionPoolSize) // do we really want this here?
	if err != nil {
		u.Error(err)
	}
	fmt.Println("Proxy after NewQuantaSource")

	schema.RegisterSourceAsSchema("quanta", src) // what does this do?  can we do it later? atw

	fmt.Println("Proxy after RegisterSourceAsSchema")

	// Start server endpoint
	portStr := strconv.FormatInt(int64(proxyHostPort), 10)
	l, err := net.Listen("tcp", "0.0.0.0:"+portStr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				u.Errorf(err.Error())
				return
			}
			go proxy.OnConn(conn)
		}
	}()

	go func(localProxy *LocalProxyControl) {

		<-localProxy.Stop
		fmt.Println("Stopping proxy")
		// and ??

	}(localProxy)

	return localProxy
}
