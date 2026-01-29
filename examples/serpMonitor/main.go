package main

import (
	"context"
	"fmt"

	f4a "github.com/futura-platform/f4a/pkg"
	"github.com/futura-platform/f4a/pkg/execute"
)

func main() {
	fmt.Println("Starting f4a runner")
	panic(f4a.Start(context.Background(), map[string]execute.Executor{}))
}
