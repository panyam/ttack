package main

import (
	"fmt"
	"os"
	"strconv"
)

func EndCommand(dataStore DataStore, args []string) {
	os.Exit(0)
}

func GetCommand(dataStore DataStore, args []string) {
	for _, arg := range args {
		val, exists := dataStore.Get(arg)
		if exists {
			fmt.Println(val)
		} else {
			fmt.Println("NULL")
		}
	}
}

func SetCommand(dataStore DataStore, args []string) {
	if len(args) > 1 {
		val, err := strconv.ParseInt(args[1], 10, 64)
		if err == nil {
			dataStore.Set(args[0], val)
		}
	}
}

func UnsetCommand(dataStore DataStore, args []string) {
	for _, arg := range args {
		dataStore.Del(arg)
	}
}

func CountCommand(dataStore DataStore, args []string) {
	for _, arg := range args {
		val, err := strconv.ParseInt(arg, 10, 64)
		if err == nil {
			fmt.Println(dataStore.Count(val))
		} else {
			fmt.Println(0)
		}
	}
}

func BeginTransactionCommand(dataStore DataStore, args []string) {
	dataStore.BeginTransaction()
}

func DebugCommand(dataStore DataStore, args []string) {
	dataStore.PrintDebug()
}

func CommitTransactionCommand(dataStore DataStore, args []string) {
	dataStore.CommitTransaction()
}

func RollbackTransactionCommand(dataStore DataStore, args []string) {
	if !dataStore.RollbackTransaction() {
		fmt.Println("NO TRANSACTION")
	}
}
