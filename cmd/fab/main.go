package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"gopkg.in/yaml.v3"

	"durarun-operator/internal/model"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	apiURL := envOr("API_URL", "http://localhost:8080")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "submit":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: fab submit <file.yaml>")
			os.Exit(1)
		}
		cmdSubmit(apiURL, os.Args[2])
	case "status":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: fab status <job-id>")
			os.Exit(1)
		}
		cmdStatus(apiURL, os.Args[2])
	case "logs":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: fab logs <job-id>")
			os.Exit(1)
		}
		cmdLogs(apiURL, os.Args[2])
	case "artifacts":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: fab artifacts <job-id>")
			os.Exit(1)
		}
		cmdArtifacts(apiURL, os.Args[2])
	case "get-artifact":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: fab get-artifact <job-id> <name>")
			os.Exit(1)
		}
		cmdGetArtifact(apiURL, os.Args[2], os.Args[3])
	case "list":
		cmdList(apiURL)
	case "pool":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: fab pool <list|describe> [args]")
			os.Exit(1)
		}
		switch os.Args[2] {
		case "list":
			cmdPoolList(apiURL)
		case "describe":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "usage: fab pool describe <name>")
				os.Exit(1)
			}
			cmdPoolDescribe(apiURL, os.Args[3])
		default:
			fmt.Fprintf(os.Stderr, "unknown pool subcommand: %s\n", os.Args[2])
			os.Exit(1)
		}
	case "describe":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: fab describe <job-id> [--steps|--recovery]")
			os.Exit(1)
		}
		jobID := os.Args[2]
		if hasFlag("--steps") {
			cmdDescribeSteps(apiURL, jobID)
		} else if hasFlag("--recovery") {
			cmdDescribeRecovery(apiURL, jobID)
		} else {
			cmdStatus(apiURL, jobID)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func hasFlag(flag string) bool {
	for _, arg := range os.Args[3:] {
		if arg == flag {
			return true
		}
	}
	return false
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: fab <command> [args]")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  submit <file.yaml>              submit a job")
	fmt.Fprintln(os.Stderr, "  status <job-id>                 show job status")
	fmt.Fprintln(os.Stderr, "  logs <job-id>                   show job logs")
	fmt.Fprintln(os.Stderr, "  artifacts <job-id>              list artifacts")
	fmt.Fprintln(os.Stderr, "  get-artifact <job-id> <name>    download artifact to stdout")
	fmt.Fprintln(os.Stderr, "  list                            list all jobs")
	fmt.Fprintln(os.Stderr, "  pool list                       list all sandbox pools")
	fmt.Fprintln(os.Stderr, "  pool describe <name>            show pool details")
	fmt.Fprintln(os.Stderr, "  describe <job-id> [--steps]     show job step progress")
	fmt.Fprintln(os.Stderr, "  describe <job-id> [--recovery]  show job recovery info")
}

func cmdSubmit(apiURL, file string) {
	data, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read file: %v\n", err)
		os.Exit(1)
	}
	var spec model.JobSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		fmt.Fprintf(os.Stderr, "parse YAML: %v\n", err)
		os.Exit(1)
	}
	body, _ := json.Marshal(spec)
	resp, err := http.Post(apiURL+"/api/v1/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "http post: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		printErrResponse(resp)
		os.Exit(1)
	}
	var result model.SubmitResponse
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("Job submitted: %s\n", result.JobID)
}

func cmdStatus(apiURL, jobID string) {
	resp, err := http.Get(apiURL + "/api/v1/jobs/" + jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	fmt.Printf("ID:        %s\n", str(result["id"]))
	fmt.Printf("State:     %s\n", str(result["state"]))
	fmt.Printf("Created:   %s\n", str(result["createdAt"]))
	fmt.Printf("Updated:   %s\n", str(result["updatedAt"]))
	if spec, ok := result["spec"].(map[string]interface{}); ok {
		fmt.Printf("Name:      %s\n", str(spec["name"]))
		fmt.Printf("Image:     %s\n", str(spec["image"]))
	}
	if attempts, ok := result["attempts"].([]interface{}); ok && len(attempts) > 0 {
		fmt.Printf("\nAttempts:\n")
		for _, a := range attempts {
			if am, ok := a.(map[string]interface{}); ok {
				fmt.Printf("  #%.0f  state=%-12s  worker=%s  started=%s\n",
					am["number"], str(am["state"]), str(am["workerId"]), str(am["startedAt"]))
			}
		}
	}
}

func cmdLogs(apiURL, jobID string) {
	resp, err := http.Get(apiURL + "/api/v1/jobs/" + jobID + "/logs")
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var logs []string
	json.NewDecoder(resp.Body).Decode(&logs)
	for _, l := range logs {
		fmt.Println(l)
	}
}

func cmdArtifacts(apiURL, jobID string) {
	resp, err := http.Get(apiURL + "/api/v1/jobs/" + jobID + "/artifacts")
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var names []string
	json.NewDecoder(resp.Body).Decode(&names)
	if len(names) == 0 {
		fmt.Println("(no artifacts)")
		return
	}
	for _, name := range names {
		fmt.Println(name)
	}
}

func cmdGetArtifact(apiURL, jobID, name string) {
	resp, err := http.Get(apiURL + "/api/v1/jobs/" + jobID + "/artifacts/" + name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	io.Copy(os.Stdout, resp.Body)
}

func cmdList(apiURL string) {
	resp, err := http.Get(apiURL + "/api/v1/jobs")
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var jobs []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobs)
	if len(jobs) == 0 {
		fmt.Println("(no jobs)")
		return
	}
	fmt.Printf("%-38s  %-12s  %-20s  %s\n", "ID", "STATE", "CREATED", "NAME")
	fmt.Printf("%-38s  %-12s  %-20s  %s\n", "------", "-----", "-------", "----")
	for _, j := range jobs {
		name := ""
		if spec, ok := j["spec"].(map[string]interface{}); ok {
			name = str(spec["name"])
		}
		fmt.Printf("%-38s  %-12s  %-20s  %s\n",
			str(j["id"]), str(j["state"]), str(j["createdAt"]), name)
	}
}

func cmdPoolList(apiURL string) {
	resp, err := http.Get(apiURL + "/api/v2/pools")
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var pools []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pools)
	if len(pools) == 0 {
		fmt.Println("(no pools)")
		return
	}
	fmt.Printf("%-20s  %-8s  %-8s  %-8s  %-6s  %s\n", "NAME", "TOTAL", "IDLE", "CLAIMED", "READY", "CREATED")
	fmt.Printf("%-20s  %-8s  %-8s  %-8s  %-6s  %s\n", "----", "-----", "----", "-------", "-----", "-------")
	for _, p := range pools {
		name := str(p["name"])
		createdAt := str(p["createdAt"])
		totalPods := ""
		idlePods := ""
		claimedPods := ""
		ready := ""
		if status, ok := p["status"].(map[string]interface{}); ok {
			totalPods = str(status["totalPods"])
			idlePods = str(status["idlePods"])
			claimedPods = str(status["claimedPods"])
			if r, ok := status["ready"].(bool); ok {
				if r {
					ready = "true"
				} else {
					ready = "false"
				}
			} else {
				ready = str(status["ready"])
			}
		}
		fmt.Printf("%-20s  %-8s  %-8s  %-8s  %-6s  %s\n", name, totalPods, idlePods, claimedPods, ready, createdAt)
	}
}

func cmdPoolDescribe(apiURL, name string) {
	resp, err := http.Get(apiURL + "/api/v2/pools/" + name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var detail map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&detail)

	fmt.Printf("Name:        %s\n", str(detail["name"]))
	fmt.Printf("Created:     %s\n", str(detail["createdAt"]))

	if config, ok := detail["config"].(map[string]interface{}); ok {
		fmt.Printf("\nConfig:\n")
		fmt.Printf("  MinSize:            %s\n", str(config["minSize"]))
		fmt.Printf("  MaxSize:            %s\n", str(config["maxSize"]))
		fmt.Printf("  ScaleUpThreshold:   %s\n", str(config["scaleUpThreshold"]))
		fmt.Printf("  ScaleDownThreshold: %s\n", str(config["scaleDownThreshold"]))
		fmt.Printf("  ScaleUpStep:        %s\n", str(config["scaleUpStep"]))
		fmt.Printf("  CooldownSeconds:    %s\n", str(config["cooldownSeconds"]))
		fmt.Printf("  Image:              %s\n", str(config["image"]))
		fmt.Printf("  RuntimeClass:       %s\n", str(config["runtimeClass"]))
	}

	if status, ok := detail["status"].(map[string]interface{}); ok {
		fmt.Printf("\nStatus:\n")
		fmt.Printf("  TotalPods:    %s\n", str(status["totalPods"]))
		fmt.Printf("  IdlePods:     %s\n", str(status["idlePods"]))
		fmt.Printf("  ClaimedPods:  %s\n", str(status["claimedPods"]))
		fmt.Printf("  Ready:        %s\n", str(status["ready"]))
		if lst := str(status["lastScaleTime"]); lst != "" && lst != "<nil>" {
			fmt.Printf("  LastScaleTime: %s\n", lst)
		}
	}
}

func cmdDescribeSteps(apiURL, jobID string) {
	resp, err := http.Get(apiURL + "/api/v2/jobs/" + jobID + "/steps")
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	fmt.Printf("Job ID:             %s\n", str(result["jobId"]))
	fmt.Printf("Completed Count:    %s\n", str(result["completedCount"]))
	fmt.Printf("Last Completed:     %s\n", str(result["lastCompletedStep"]))

	if steps, ok := result["steps"].([]interface{}); ok && len(steps) > 0 {
		fmt.Printf("\nSteps:\n")
		fmt.Printf("  %-20s  %-12s  %-8s  %-8s  %s\n", "STEP_ID", "TYPE", "SEQ", "EXIT", "OUTPUT_REF")
		fmt.Printf("  %-20s  %-12s  %-8s  %-8s  %s\n", "-------", "----", "---", "----", "----------")
		for _, s := range steps {
			if sm, ok := s.(map[string]interface{}); ok {
				exitCode := ""
				if ec, ok := sm["exitCode"]; ok && ec != nil {
					exitCode = str(ec)
				}
				fmt.Printf("  %-20s  %-12s  %-8s  %-8s  %s\n",
					str(sm["stepId"]),
					str(sm["type"]),
					str(sm["seq"]),
					exitCode,
					str(sm["outputRef"]),
				)
			}
		}
	} else {
		fmt.Println("\n(no steps)")
	}
}

func cmdDescribeRecovery(apiURL, jobID string) {
	resp, err := http.Get(apiURL + "/api/v2/jobs/" + jobID + "/recovery")
	if err != nil {
		fmt.Fprintf(os.Stderr, "http get: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		printErrResponse(resp)
		os.Exit(1)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	fmt.Printf("Job ID:             %s\n", str(result["jobId"]))
	fmt.Printf("Recovery Mode:      %s\n", str(result["recoveryMode"]))
	fmt.Printf("Last Completed:     %s\n", str(result["lastCompletedStep"]))
	fmt.Printf("Recovered From:     %s\n", str(result["recoveredFromStep"]))

	if steps, ok := result["completedSteps"].([]interface{}); ok && len(steps) > 0 {
		fmt.Printf("\nCompleted Steps:\n")
		for _, s := range steps {
			fmt.Printf("  - %s\n", str(s))
		}
	} else {
		fmt.Println("\n(no completed steps)")
	}
}

func printErrResponse(resp *http.Response) {
	var e model.ErrorResponse
	json.NewDecoder(resp.Body).Decode(&e)
	if e.Error != "" {
		fmt.Fprintf(os.Stderr, "error (HTTP %d): %s\n", resp.StatusCode, e.Error)
	} else {
		fmt.Fprintf(os.Stderr, "error: HTTP %d\n", resp.StatusCode)
	}
}

func str(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
