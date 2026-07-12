//go:build kvnode

package main

import (
	"context"
	"net"
	"strings"
	"testing"
)

func TestHardenedServiceFlagValidation(t *testing.T) {
	listener := func(string, string) (net.Listener, error) {
		t.Fatal("invalid configuration reached listener creation")
		return nil, nil
	}
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "protocol tick lower bound", args: []string{"-protocol-tick-ms=0"}, want: "protocol tick"},
		{name: "protocol tick upper bound", args: []string{"-protocol-tick-ms=60001"}, want: "protocol tick"},
		{name: "queue capacity", args: []string{"-transport-queue-capacity=0"}, want: "transport queue capacity"},
		{name: "retry capacity", args: []string{"-transport-retry-capacity=1048577"}, want: "transport retry capacity"},
		{name: "workers", args: []string{"-transport-workers=257"}, want: "transport workers"},
		{name: "shutdown timeout", args: []string{"-shutdown-timeout-ms=300001"}, want: "shutdown timeout"},
		{name: "idle connections", args: []string{"-peer-max-idle-conns-per-host=0"}, want: "peer max idle"},
		{name: "connections", args: []string{"-peer-max-conns-per-host=4097"}, want: "peer max connections"},
		{name: "idle exceeds total", args: []string{"-peer-max-idle-conns-per-host=17", "-peer-max-conns-per-host=16"}, want: "must not exceed"},
		{name: "Pebble memtables", args: []string{"-pebble-memtable-stop-writes=1"}, want: "invalid Pebble"},
		{name: "production TLS required", args: []string{"-production", "-max-peer-body-bytes=2097152", "-retention-max-resident-instances=10", "-retention-max-durable-records=10", "-retention-max-data-bytes=1048576"}, want: "requires complete"},
		{name: "shared TLS flags removed", args: []string{"-tls-cert=removed"}, want: "flag provided but not defined"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runKVNode(context.Background(), test.args, listener)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runKVNode error=%v, want %q", err, test.want)
			}
		})
	}
}
