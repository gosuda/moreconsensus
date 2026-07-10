//go:build !darwin

package main

import (
	"context"
	"fmt"
)

func pausePID(pid int) error {
	return fmt.Errorf("process pause is available only on native Darwin")
}

func resumePID(pid int) error {
	return fmt.Errorf("process resume is available only on native Darwin")
}

func sampleDarwinProcess(ctx context.Context, pid int) (uint64, int, error) {
	return 0, 0, fmt.Errorf("process resource sampling is available only on native Darwin")
}
