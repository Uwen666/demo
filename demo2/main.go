package main

import (
	"blockEmulator/build"
	"blockEmulator/params"
	"fmt"
	"log"

	"github.com/spf13/pflag"
)

var (
	// network config
	shardNum int
	nodeNum  int
	shardID  int
	nodeID   int
	standbyID int
	bindAddr  string

	// supervisor or not
	isSupervisor bool
	isStandby    bool

	// batch running config
	isGen                bool
	isGenerateForExeFile bool
)

func main() {
	// Read basic configs
	params.ReadConfigFile()

	// Generate bat files
	pflag.BoolVarP(&isGen, "gen", "g", false, "isGen is a bool value, which indicates whether to generate a batch file")
	pflag.BoolVarP(&isGenerateForExeFile, "shellForExe", "f", false, "isGenerateForExeFile is a bool value, which is effective only if 'isGen' is true; True to generate for an executable, False for 'go run'. ")

	// Start a node.
	pflag.IntVarP(&shardNum, "shardNum", "S", params.ShardNum, "shardNum is an Integer, which indicates that how many shards are deployed. ")
	pflag.IntVarP(&nodeNum, "nodeNum", "N", params.NodesInShard, "nodeNum is an Integer, which indicates how many nodes of each shard are deployed. ")
	pflag.IntVarP(&shardID, "shardID", "s", -1, "shardID is an Integer, which indicates the ID of the shard to which this node belongs. Value range: [0, shardNum). ")
	pflag.IntVarP(&nodeID, "nodeID", "n", -1, "nodeID is an Integer, which indicates the ID of this node. Value range: [0, nodeNum).")
	pflag.BoolVarP(&isSupervisor, "supervisor", "c", false, "isSupervisor is a bool value, which indicates whether this node is a supervisor.")
	pflag.BoolVar(&isStandby, "standby", false, "start this process as a standby candidate node (not in CS/SS at boot)")
	pflag.IntVar(&standbyID, "standbyID", -1, "standby candidate id, used for unique pseudo-shard and log/db path")
	pflag.StringVar(&bindAddr, "bindAddr", "", "bind address for standby candidate node, e.g. 127.0.0.1:18889")

	pflag.Parse()

	if isGen {
		if isGenerateForExeFile {
			// Generate the corresponding .bat file or .sh file based on the detected operating system.
			if err := build.GenerateExeBatchByIpTable(nodeNum, shardNum); err != nil {
				fmt.Println(err.Error())
			}
		} else {
			// Generate a .bat file or .sh file for running `go run`.
			if err := build.GenerateBatchByIpTable(nodeNum, shardNum); err != nil {
				fmt.Println(err.Error())
			}
		}
		return
	}

	if isSupervisor {
		build.BuildSupervisor(uint64(nodeNum), uint64(shardNum))
	} else if isStandby {
		if standbyID < 0 {
			log.Panic("standbyID must be >= 0 when --standby is used")
		}
		if bindAddr == "" {
			log.Panic("bindAddr is required when --standby is used")
		}
		build.BuildStandbyPbftNode(uint64(standbyID), bindAddr, uint64(nodeNum), uint64(shardNum))
	} else {
		if shardID >= shardNum || shardID < 0 {
			log.Panicf("Wrong ShardID. This ShardID is %d, but only %d shards in the current config. ", shardID, shardNum)
		}
		if nodeID >= nodeNum || nodeID < 0 {
			log.Panicf("Wrong NodeID. This NodeID is %d, but only %d nodes in the current config. ", nodeID, nodeNum)
		}
		build.BuildNewPbftNode(uint64(nodeID), uint64(nodeNum), uint64(shardID), uint64(shardNum))
	}
}
