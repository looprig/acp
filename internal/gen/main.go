// Command gen reads a pinned ACP schema.json + meta.json pair and writes
// protocol/types_gen.go and protocol/methods_gen.go. It never fetches
// anything from the network; the schema artifacts are pinned and reviewed
// separately (see protocol/schema/v1/REVISION).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	schemaPath := fs.String("schema", "", "path to the pinned ACP schema.json")
	metaPath := fs.String("meta", "", "path to the pinned ACP meta.json")
	outDir := fs.String("out", ".", "output directory for types_gen.go and methods_gen.go")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *schemaPath == "" || *metaPath == "" {
		return fmt.Errorf("-schema and -meta are required")
	}

	schemaBytes, err := os.ReadFile(*schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	metaBytes, err := os.ReadFile(*metaPath)
	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}

	model, err := BuildModel(schemaBytes, metaBytes)
	if err != nil {
		return fmt.Errorf("build model: %w", err)
	}

	typesSrc, err := EmitTypes(model)
	if err != nil {
		return fmt.Errorf("emit types: %w", err)
	}
	methodsSrc, err := EmitMethods(model)
	if err != nil {
		return fmt.Errorf("emit methods: %w", err)
	}

	if err := os.WriteFile(filepath.Join(*outDir, "types_gen.go"), typesSrc, 0o600); err != nil {
		return fmt.Errorf("write types_gen.go: %w", err)
	}
	if err := os.WriteFile(filepath.Join(*outDir, "methods_gen.go"), methodsSrc, 0o600); err != nil {
		return fmt.Errorf("write methods_gen.go: %w", err)
	}
	return nil
}
