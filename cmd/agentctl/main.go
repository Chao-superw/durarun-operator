package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "durarun-operator/api/v1alpha1"
)

const appVersion = "v0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	if cmd == "version" {
		fmt.Printf("agentctl %s\n", appVersion)
		return
	}

	c, err := buildClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	switch cmd {
	case "submit":
		cmdSubmit(c, os.Args[2:])
	case "get":
		cmdGet(c, os.Args[2:])
	case "describe":
		cmdDescribe(c, os.Args[2:])
	case "delete":
		cmdDelete(c, os.Args[2:])
	case "cancel":
		cmdCancel(c, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func buildClient() (client.Client, error) {
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		return nil, fmt.Errorf("register core/v1: %w", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		return nil, fmt.Errorf("register v1alpha1: %w", err)
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}

	return client.New(cfg, client.Options{Scheme: s})
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: agentctl <command> [args]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  submit   -f <file.yaml> [-n namespace]   create an AgentJob from YAML")
	fmt.Fprintln(os.Stderr, "  get      [-n namespace]                  list AgentJobs")
	fmt.Fprintln(os.Stderr, "  describe <name> [-n namespace]           show AgentJob details")
	fmt.Fprintln(os.Stderr, "  delete   <name> [-n namespace]           delete an AgentJob")
	fmt.Fprintln(os.Stderr, "  cancel   <name> [-n namespace]           cancel a running AgentJob")
	fmt.Fprintln(os.Stderr, "  version                                  print version")
}

// parseArgs extracts the -n namespace flag and collects positional arguments
// from a mixed argument list (supports both "name -n ns" and "-n ns name").
func parseArgs(args []string) (positional []string, namespace string) {
	namespace = "default"
	for i := 0; i < len(args); i++ {
		if args[i] == "-n" && i+1 < len(args) {
			namespace = args[i+1]
			i++
		} else {
			positional = append(positional, args[i])
		}
	}
	return
}

// --- submit ---

func cmdSubmit(c client.Client, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	file := fs.String("f", "", "YAML file path")
	namespace := fs.String("n", "default", "namespace")
	fs.Parse(args)

	if *file == "" {
		fmt.Fprintln(os.Stderr, "error: -f flag is required")
		fmt.Fprintln(os.Stderr, "usage: agentctl submit -f <file.yaml> [-n namespace]")
		os.Exit(1)
	}

	data, err := os.ReadFile(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
		os.Exit(1)
	}

	job := &v1alpha1.AgentJob{}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), len(data))
	if err := decoder.Decode(job); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing YAML: %v\n", err)
		os.Exit(1)
	}

	if job.Namespace == "" {
		job.Namespace = *namespace
	}

	ctx := context.Background()
	if err := c.Create(ctx, job); err != nil {
		fmt.Fprintf(os.Stderr, "error creating AgentJob: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("AgentJob %s created\n", job.Name)
}

// --- get ---

func cmdGet(c client.Client, args []string) {
	_, namespace := parseArgs(args)

	var list v1alpha1.AgentJobList
	ctx := context.Background()
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		fmt.Fprintf(os.Stderr, "error listing AgentJobs: %v\n", err)
		os.Exit(1)
	}

	if len(list.Items) == 0 {
		fmt.Println("No AgentJobs found")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "NAME\tPHASE\tATTEMPTS\tAGE\n")
	for _, job := range list.Items {
		age := formatDuration(time.Since(job.CreationTimestamp.Time))
		attempts := job.Status.CompletedAttempts + job.Status.FailedAttempts
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", job.Name, job.Status.Phase, attempts, age)
	}
	w.Flush()
}

// --- describe ---

func cmdDescribe(c client.Client, args []string) {
	positional, namespace := parseArgs(args)
	if len(positional) < 1 {
		fmt.Fprintln(os.Stderr, "error: job name is required")
		fmt.Fprintln(os.Stderr, "usage: agentctl describe <name> [-n namespace]")
		os.Exit(1)
	}
	name := positional[0]
	ctx := context.Background()

	// Get the AgentJob.
	job := &v1alpha1.AgentJob{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, job); err != nil {
		fmt.Fprintf(os.Stderr, "error getting AgentJob: %v\n", err)
		os.Exit(1)
	}

	// Basic info.
	fmt.Printf("Name:         %s\n", job.Name)
	fmt.Printf("Namespace:    %s\n", job.Namespace)
	fmt.Printf("Phase:        %s\n", job.Status.Phase)
	fmt.Printf("Age:          %s\n", formatDuration(time.Since(job.CreationTimestamp.Time)))
	if job.Status.StartTime != nil {
		fmt.Printf("Start Time:   %s\n", job.Status.StartTime.Format(time.RFC3339))
	}
	if job.Status.CompletionTime != nil {
		fmt.Printf("Completed:    %s\n", job.Status.CompletionTime.Format(time.RFC3339))
	}

	// Spec.
	fmt.Printf("\nSpec:\n")
	fmt.Printf("  Image:        %s\n", job.Spec.Image)
	if len(job.Spec.Command) > 0 {
		fmt.Printf("  Command:      %v\n", job.Spec.Command)
	}
	if job.Spec.Prompt != "" {
		fmt.Printf("  Prompt:       %s\n", job.Spec.Prompt)
	}
	fmt.Printf("  Timeout:      %s\n", job.Spec.Timeout)
	fmt.Printf("  Max Retries:  %d\n", job.Spec.MaxRetries)
	fmt.Printf("  Pool Ref:     %s\n", job.Spec.PoolRef)
	fmt.Printf("  Isolation:    %s\n", job.Spec.Isolation.Level)
	fmt.Printf("  Resources:    CPU=%s, Memory=%s\n",
		job.Spec.Resources.CPULimit, job.Spec.Resources.MemoryLimit)

	// Status.
	fmt.Printf("\nStatus:\n")
	fmt.Printf("  Completed Attempts: %d\n", job.Status.CompletedAttempts)
	fmt.Printf("  Failed Attempts:    %d\n", job.Status.FailedAttempts)
	if job.Status.ActiveAttempt != nil {
		fmt.Printf("  Active Attempt:     %s (#%d)\n",
			job.Status.ActiveAttempt.Name, job.Status.ActiveAttempt.Number)
	}

	// Conditions.
	if len(job.Status.Conditions) > 0 {
		fmt.Printf("\nConditions:\n")
		for _, cond := range job.Status.Conditions {
			fmt.Printf("  %s: %s (reason: %s)\n", cond.Type, cond.Status, cond.Reason)
		}
	}

	// List AgentAttempts belonging to this job (filter by spec.jobRef).
	var attemptList v1alpha1.AgentAttemptList
	if err := c.List(ctx, &attemptList, client.InNamespace(namespace)); err != nil {
		fmt.Fprintf(os.Stderr, "\nwarning: could not list attempts: %v\n", err)
		return
	}

	var jobAttempts []v1alpha1.AgentAttempt
	for _, a := range attemptList.Items {
		if a.Spec.JobRef == name {
			jobAttempts = append(jobAttempts, a)
		}
	}

	if len(jobAttempts) > 0 {
		fmt.Printf("\nAttempts:\n")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "  NAME\tNUMBER\tPHASE\tPOD\tAGE\n")
		for _, a := range jobAttempts {
			age := formatDuration(time.Since(a.CreationTimestamp.Time))
			fmt.Fprintf(w, "  %s\t%d\t%s\t%s\t%s\n",
				a.Name, a.Spec.Number, a.Status.Phase, a.Status.PodName, age)
		}
		w.Flush()
	}
}

// --- delete ---

func cmdDelete(c client.Client, args []string) {
	positional, namespace := parseArgs(args)
	if len(positional) < 1 {
		fmt.Fprintln(os.Stderr, "error: job name is required")
		fmt.Fprintln(os.Stderr, "usage: agentctl delete <name> [-n namespace]")
		os.Exit(1)
	}
	name := positional[0]

	job := &v1alpha1.AgentJob{}
	job.Name = name
	job.Namespace = namespace

	ctx := context.Background()
	if err := c.Delete(ctx, job); err != nil {
		fmt.Fprintf(os.Stderr, "error deleting AgentJob: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("AgentJob %s deleted\n", name)
}

// --- cancel ---

func cmdCancel(c client.Client, args []string) {
	positional, namespace := parseArgs(args)
	if len(positional) < 1 {
		fmt.Fprintln(os.Stderr, "error: job name is required")
		fmt.Fprintln(os.Stderr, "usage: agentctl cancel <name> [-n namespace]")
		os.Exit(1)
	}
	name := positional[0]
	ctx := context.Background()

	// Get current job.
	job := &v1alpha1.AgentJob{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, job); err != nil {
		fmt.Fprintf(os.Stderr, "error getting AgentJob: %v\n", err)
		os.Exit(1)
	}

	// Refuse if already in a terminal phase.
	switch job.Status.Phase {
	case v1alpha1.JobPhaseSucceeded, v1alpha1.JobPhaseFailed, v1alpha1.JobPhaseTerminated:
		fmt.Fprintf(os.Stderr, "AgentJob %s is already in terminal phase: %s\n", name, job.Status.Phase)
		os.Exit(1)
	}

	// Set phase to Terminated.
	job.Status.Phase = v1alpha1.JobPhaseTerminated
	if err := c.Status().Update(ctx, job); err != nil {
		fmt.Fprintf(os.Stderr, "error updating AgentJob status: %v\n", err)
		os.Exit(1)
	}

	// Delete the active pod if one exists.
	if job.Status.ActiveAttempt != nil {
		attempt := &v1alpha1.AgentAttempt{}
		attemptKey := client.ObjectKey{Namespace: namespace, Name: job.Status.ActiveAttempt.Name}
		if err := c.Get(ctx, attemptKey, attempt); err == nil && attempt.Status.PodName != "" {
			pod := &corev1.Pod{}
			pod.Name = attempt.Status.PodName
			pod.Namespace = namespace
			if err := c.Delete(ctx, pod); err != nil {
				fmt.Fprintf(os.Stderr, "warning: could not delete pod %s: %v\n",
					attempt.Status.PodName, err)
			} else {
				fmt.Printf("Pod %s deleted\n", attempt.Status.PodName)
			}
		}
	}

	fmt.Printf("AgentJob %s cancelled\n", name)
}

// --- helpers ---

func formatDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
