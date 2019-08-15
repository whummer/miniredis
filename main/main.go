package main

import (
  "os"
  "time"
  "log"
  "strconv"
  miniredis "github.com/alicebob/miniredis"
)

func forever() {
    for {
        time.Sleep(time.Second)
    }
}

func main() {
  m := miniredis.NewMiniRedis()
  port, _ := strconv.Atoi(os.Args[1])
  err := m.StartPort(port)

  if err != nil {
    log.Printf("Error %s", err)
    os.Exit(1)
  }

  defer m.Close()

  go forever()
  select {}
}
