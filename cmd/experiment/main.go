package main

import (
	"flag"
	"fmt"
	"os"

	"durarun-operator/experiment"
)

func main() {
	trials := flag.Int("trials", 5, "number of trials per combination")
	outDir := flag.String("out", "", "output directory for report")
	temporalAddr := flag.String("temporal-addr", "localhost:17233", "Temporal dev server address (host:port)")
	flag.Parse()

	report := experiment.RunFullExperiment(*trials, *temporalAddr)
	report.PrintSummary()

	if *outDir != "" {
		if err := experiment.SaveAll(report, *outDir); err != nil {
			fmt.Fprintf(os.Stderr, "save: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nReport saved to: %s/\n", *outDir)
	}
}
