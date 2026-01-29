package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func RequiredPort(name string) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("invalid %s: %q", name, value)
	}
	return port, nil
}
