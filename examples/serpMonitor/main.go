package main

import (
	"fmt"

	f4a "github.com/futura-platform/f4a/pkg"
	"github.com/futura-platform/f4a/pkg/execute"
)

func main() {
	fmt.Println("Starting f4a runner")
	panic(f4a.Start(":8080", map[string]execute.Executor{}))
}
