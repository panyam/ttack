package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

type DataStore interface {
	Get(key string) (int64, bool)
	Set(key string, value int64)
	Del(key string)
	Count(value int64) int
	BeginTransaction()
	RollbackTransaction() bool
	CommitTransaction()
	PrintDebug()
}

type Command func(dataStore DataStore, args []string)

func main() {
	commandsTable := map[string]Command{
		"END":        EndCommand,
		"GET":        GetCommand,
		"SET":        SetCommand,
		"UNSET":      UnsetCommand,
		"NUMEQUALTO": CountCommand,
		"BEGIN":      BeginTransactionCommand,
		"COMMIT":     CommitTransactionCommand,
		"ROLLBACK":   RollbackTransactionCommand,
		"DEBUG":      DebugCommand,
	}

	dataStore := NewDataStore()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		text = strings.Trim(text, " \t\n")

		if len(text) > 0 {
			// parse into right command
			parts := strings.Fields(text)
			cmd := commandsTable[strings.ToUpper(parts[0])]
			if cmd != nil {
				cmd(dataStore, parts[1:])
			} else {
				log.Println("Invalid command: ", parts[0])
				return
			}
		}
	}
	return
}
