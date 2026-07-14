//go:build !darwin

package main

import (
	"context"
	"fmt"
)

func sampleDarwinProcess(_ context.Context, _ int) (uint64, int, error) {
	return 0, 0, fmt.Errorf("process resource sampling is available only on native Darwin")
}
