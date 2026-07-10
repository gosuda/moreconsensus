package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "deploymentcollector: %v\n", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("usage: deploymentcollector <inspect|preboot|postboot|assemble-input|finalize|verify-rehearsal> --config /absolute/config.json")
	}
	stage := arguments[0]
	flags := flag.NewFlagSet(stage, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "", "absolute strict JSON collector configuration")
	if err := flags.Parse(arguments[1:]); err != nil {
		return err
	}
	if *configPath == "" || len(flags.Args()) != 0 {
		return errors.New("stage requires exactly --config /absolute/config.json")
	}
	config, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	runner := nativeRunner{}
	var result any
	switch stage {
	case "inspect":
		result, err = inspect(config, runner)
	case "preboot":
		result, err = preboot(config, runner)
	case "postboot":
		result, err = postboot(config, runner)
	case "assemble-input":
		var path string
		path, err = assembleInput(config, runner)
		result = map[string]string{"input_path": path}
	case "finalize":
		result, err = finalize(config, runner)
	case "verify-rehearsal":
		result, err = verifyRehearsal(config, runner)
	default:
		return fmt.Errorf("unknown stage %q", stage)
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}
