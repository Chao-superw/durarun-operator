// Package main implements a simple test workload that writes numbered files
// to simulate a multi-step agent. Each step writes a file into the work
// directory with a brief delay between steps.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	workDir := flag.String("work-dir", "/workspace", "work directory")
	steps := flag.Int("steps", 5, "number of steps")
	delay := flag.Duration("delay", time.Second, "delay between steps")
	flag.Parse()

	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create work dir: %v\n", err)
		os.Exit(1)
	}

	for i := 0; i < *steps; i++ {
		filename := filepath.Join(*workDir, fmt.Sprintf("step-%03d.txt", i))
		content := fmt.Sprintf("step %d completed at %s\n", i, time.Now().Format(time.RFC3339Nano))
		if err := os.WriteFile(filename, []byte(content), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write step %d: %v\n", i, err)
			os.Exit(1)
		}
		fmt.Printf("step %d: wrote %s\n", i, filename)
		if i < *steps-1 {
			time.Sleep(*delay)
		}
	}
	fmt.Println("all steps completed")
}
