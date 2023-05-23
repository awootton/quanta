package server

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

var count = 0

// verify - Verify node state. and dump to file or stdout.
func Verify(node *Node) string {

	var sb strings.Builder
	indent := 0

	sb.WriteString("dataDir " + node.dataDir + "\n")
	sb.WriteString("hashKey " + node.hashKey + "\n")
	sb.WriteString("State " + node.State.String() + "\n")
	sb.WriteString("len(tables) " + strconv.FormatInt(int64(len(node.TableCache.TableCache)), 10) + "\n")
	for _, t := range node.TableCache.TableCache {
		sb.WriteString("    table " + t.GetName() + "\n")
	}
	sb.WriteString("len(node.localServices) " + strconv.FormatInt(int64(len(node.localServices)), 10) + "\n")
	for k, v := range node.localServices {
		sb.WriteString("    localService " + k + " " + v.GetName() + "\n")
	}

	sb.WriteString("nodeMap " + fmt.Sprint(node.GetNodeMap()) + "\n")
	sb.WriteString("isLocal " + fmt.Sprint(node.IsLocalCluster) + "\n")

	write(node, sb)
	_ = indent
	return sb.String()
}

func write(node *Node, sb strings.Builder) {
	countStr := strconv.Itoa(100 + count)
	countStr = countStr[1:]
	fname := "dump_" + node.hashKey + "_" + countStr + ".txt"
	os.WriteFile(fname, []byte(sb.String()), 0644)
}
