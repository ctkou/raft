package cluster_api

import (
  "encoding/json"
  "log"
  "strconv"
  "net/http"
)

func StartAPI(port, nodeCount int) {
  http.HandleFunc("/", requestHandler)
  log.Fatal(http.ListenAndServe(":" + strconv.Itoa(port), nil))
}

func requestHandler(writer http.ResponseWriter, request *http.Request) {
  command := request.URL.Path[1:]
  handleCommand(writer, command)
}

func handleCommand(writer http.ResponseWriter, command string) {
  var payload []byte
  var err error

  if command == "add" {
    payload, err = json.Marshal(CommandResponse{command, "Valued added."})
  } else {
    payload, err = json.Marshal(CommandResponse{command, "Invalid Command."})
  }

  if err != nil {
    http.Error(writer, err.Error(), http.StatusInternalServerError)
  }

  writer.Header().Set("Content-Type", "application/json")
  writer.Write(payload)
}
