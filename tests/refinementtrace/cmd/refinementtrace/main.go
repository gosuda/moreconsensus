package main

import (
	"flag"
	"fmt"
	"os"

	"gosuda.org/moreconsensus/tests/refinementtrace"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: refinementtrace capture|replay [flags]")
	}
	switch os.Args[1] {
	case "capture":
		capture(os.Args[2:])
	case "replay":
		replay(os.Args[2:])
	default:
		fail("unknown command %q", os.Args[1])
	}
}

func capture(args []string) {
	flags := flag.NewFlagSet("capture", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	source := flags.String("source-revision", "", "clean full source revision")
	specPath := flags.String("spec", "tla/EPaxosRawNodeRefinement.tla", "exact formal specification path")
	outPath := flags.String("out", "", "trace export output path")
	if err := flags.Parse(args); err != nil {
		os.Exit(2)
	}
	if flags.NArg() != 0 || *outPath == "" {
		fail("capture requires -source-revision and -out, with no positional arguments")
	}
	spec, err := os.ReadFile(*specPath)
	if err != nil {
		fail("read formal spec: %v", err)
	}
	export, err := refinementtrace.Capture(*source, spec)
	if err != nil {
		fail("capture: %v", err)
	}
	if err := os.WriteFile(*outPath, export, 0o600); err != nil {
		fail("write trace export: %v", err)
	}
}

func replay(args []string) {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	source := flags.String("source-revision", "", "clean full source revision")
	specPath := flags.String("spec", "tla/EPaxosRawNodeRefinement.tla", "exact formal specification path")
	inPath := flags.String("in", "", "trace export input path")
	if err := flags.Parse(args); err != nil {
		os.Exit(2)
	}
	if flags.NArg() != 0 || *inPath == "" {
		fail("replay requires -source-revision and -in, with no positional arguments")
	}
	spec, err := os.ReadFile(*specPath)
	if err != nil {
		fail("read formal spec: %v", err)
	}
	export, err := os.ReadFile(*inPath)
	if err != nil {
		fail("read trace export: %v", err)
	}
	marker, err := refinementtrace.Replay(export, spec, *source)
	if err != nil {
		fail("replay: %v", err)
	}
	fmt.Println(marker)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
