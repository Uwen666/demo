package build

import (
	"blockEmulator/params"
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

func splitHostPort(ip string) (string, int, bool) {
	idx := strings.LastIndex(ip, ":")
	if idx <= 0 || idx >= len(ip)-1 {
		return "", 0, false
	}
	port, err := strconv.Atoi(ip[idx+1:])
	if err != nil {
		return "", 0, false
	}
	return ip[:idx], port, true
}

func standbyReserveCount(nodenum, shardnum int) int {
	csCount := shardnum
	if params.CommitteeMethod[params.ConsensusMethod] == "Reputation" && shardnum >= 2 {
		csCount = shardnum / 2
	}
	if csCount <= 0 {
		csCount = 1
	}
	if nodenum <= 0 {
		nodenum = 1
	}
	return csCount * nodenum
}

func buildStandbyAddresses(ipMap map[uint64]map[uint64]string, count int) []string {
	if count <= 0 {
		return nil
	}
	host := "127.0.0.1"
	maxPort := 0

	for _, nodes := range ipMap {
		for _, ip := range nodes {
			if h, p, ok := splitHostPort(ip); ok {
				host = h
				if p > maxPort {
					maxPort = p
				}
			}
		}
	}

	addrs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		addrs = append(addrs, fmt.Sprintf("%s:%d", host, maxPort+1+i))
	}
	return addrs
}

func GenerateBatchByIpTable(nodenum, shardnum int) error {
	// read IP table file first
	ipMap := readIpTable("./ipTable.json")

	// determine the formats of commands and fileNames, according to operating system
	var fileNameFormat, commandFormat string
	os := runtime.GOOS
	switch os {
	case "windows":
		fileNameFormat = "complie_run_IpAddr=%s.bat"
		commandFormat = "start cmd /k go run main.go"
	default:
		fileNameFormat = "complie_run_IpAddr=%s.sh"
		commandFormat = "go run main.go"
	}

	// generate file for each ip
	for i := 0; i < shardnum; i++ {
		// if this shard is not existed, return
		if _, shard_exist := ipMap[uint64(i)]; !shard_exist {
			return fmt.Errorf("the shard (shardID = %d) is not existed in the IP Table file", i)
		}
		// if this shard is existed.
		for j := 0; j < nodenum; j++ {
			if nodeIp, node_exist := ipMap[uint64(i)][uint64(j)]; node_exist {
				// attach this command to this file
				ipAddr := strings.Split(nodeIp, ":")[0]
				batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
				command := fmt.Sprintf(commandFormat+" -n %d -N %d -s %d -S %d & \n", j, nodenum, i, shardnum)
				if err := attachLineToFile(batFilePath, command); nil != err {
					return err
				}
			} else {
				return fmt.Errorf("the node (shardID = %d, nodeID = %d) is not existed in the IP Table file", i, j)
			}
		}
	}

	// generate command for supervisor first to avoid missing committee in constrained environments.
	if supervisorShard, shard_exist := ipMap[2147483647]; shard_exist {
		if nodeIp, node_exist := supervisorShard[0]; node_exist {
			ipAddr := strings.Split(nodeIp, ":")[0]
			batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
			supervisor_command := fmt.Sprintf(commandFormat+" -c -N %d -S %d & \n", nodenum, shardnum)
			if err := attachLineToFile(batFilePath, supervisor_command); nil != err {
				return err
			}
		}
	} else {
		return fmt.Errorf("the supervisor (shardID = 2147483647, nodeID = 0) is not existed in the IP Table file")
	}

	if params.CommitteeMethod[params.ConsensusMethod] == "Reputation" {
		reserveCount := standbyReserveCount(nodenum, shardnum)
		standbyAddrs := buildStandbyAddresses(ipMap, reserveCount)
		if len(standbyAddrs) > 0 {
			ipAddr := strings.Split(standbyAddrs[0], ":")[0]
			batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
			for idx, addr := range standbyAddrs {
				standbyCmd := fmt.Sprintf(commandFormat+" --standby --standbyID %d --bindAddr %s -N %d -S %d & \n", idx, addr, nodenum, shardnum)
				if err := attachLineToFile(batFilePath, standbyCmd); nil != err {
					return err
				}
			}
		}
	}

	return nil
}

func GenerateExeBatchByIpTable(nodenum, shardnum int) error {
	// read IP table file first
	ipMap := readIpTable("./ipTable.json")

	// determine the formats of commands and fileNames, according to operating system
	var fileNameFormat, commandFormat string
	os := runtime.GOOS
	switch os {
	case "windows":
		fileNameFormat = os + "_exe_run_IpAddr=%s.bat"
		commandFormat = "start cmd /k blockEmulator.exe"
	default:
		fileNameFormat = os + "_exe_run_IpAddr=%s.sh"
		commandFormat = "./blockEmulator"
	}

	// generate file for each ip
	for i := 0; i < shardnum; i++ {
		// if this shard is not existed, return
		if _, shard_exist := ipMap[uint64(i)]; !shard_exist {
			return fmt.Errorf("the shard (shardID = %d) is not existed in the IP Table file", i)
		}
		// if this shard is existed.
		for j := 0; j < nodenum; j++ {
			if nodeIp, node_exist := ipMap[uint64(i)][uint64(j)]; node_exist {
				// attach this command to this file
				ipAddr := strings.Split(nodeIp, ":")[0]
				batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
				command := fmt.Sprintf(commandFormat+" -n %d -N %d -s %d -S %d & \n", j, nodenum, i, shardnum)
				if err := attachLineToFile(batFilePath, command); nil != err {
					return err
				}
			} else {
				return fmt.Errorf("the node (shardID = %d, nodeID = %d) is not existed in the IP Table file", i, j)
			}
		}
	}

	// generate command for supervisor first to avoid missing committee in constrained environments.
	if supervisorShard, shard_exist := ipMap[2147483647]; shard_exist {
		if nodeIp, node_exist := supervisorShard[0]; node_exist {
			ipAddr := strings.Split(nodeIp, ":")[0]
			batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
			supervisor_command := fmt.Sprintf(commandFormat+" -c -N %d -S %d & \n", nodenum, shardnum)
			if err := attachLineToFile(batFilePath, supervisor_command); nil != err {
				return err
			}
		}
	} else {
		return fmt.Errorf("the supervisor (shardID = 2147483647, nodeID = 0) is not existed in the IP Table file")
	}

	if params.CommitteeMethod[params.ConsensusMethod] == "Reputation" {
		reserveCount := standbyReserveCount(nodenum, shardnum)
		standbyAddrs := buildStandbyAddresses(ipMap, reserveCount)
		if len(standbyAddrs) > 0 {
			ipAddr := strings.Split(standbyAddrs[0], ":")[0]
			batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
			for idx, addr := range standbyAddrs {
				standbyCmd := fmt.Sprintf(commandFormat+" --standby --standbyID %d --bindAddr %s -N %d -S %d & \n", idx, addr, nodenum, shardnum)
				if err := attachLineToFile(batFilePath, standbyCmd); nil != err {
					return err
				}
			}
		}
	}

	return nil
}
