package main

import (
  "fmt"
  "strconv"
  "os"
  api "raft/cluster_api"
)

func main() {
  args := os.Args
  port, nodeCount, err := getValidatedArguments(args)

  if err != nil {
    fmt.Println(err.Error())
    return
  }

  api.StartAPI(port, nodeCount)
}

func getValidatedArguments(args []string) (int, int, error) {
  if len(args) != 3 {
    return 0, 0, InputError{"to start a cluster, run 'cluster [port] [# of nodes]'."}
  }
  port, portError := getPort(args[1])
  if portError != nil {
    return 0, 0, portError
  }
  nodeCount, nodeCountError := getNodeCount(args[2])
  if nodeCountError != nil {
    return 0, 0, nodeCountError
  }
  return port, nodeCount, nil
}

func getPort(portStr string) (int, error) {
  port, portErr := strconv.Atoi(portStr)
  if portErr != nil {
    return 0, InputError{"port number must be numeric."}
  }
  if port < 1 || port > 65535 {
    return 0, InputError{"port number must be in the range of [1, 65535]."}
  }
  return port, nil
}

func getNodeCount(nodeCountStr string) (int, error) {
  nodeCount, nodeErr := strconv.Atoi(nodeCountStr)
  if nodeErr != nil {
    return 0, InputError{"the number of nodes must be numeric."}
  }
  if nodeCount < 3 || nodeCount > 100 {
    return 0, InputError{"the number of nodes must be in the range of [3, 1000]."}
  }
  return nodeCount, nil
}
