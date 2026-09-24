package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"durarun-operator/internal/artifact"
	"durarun-operator/internal/gateway"
)

func main() {
	var (
		addr        string
		storagePath string
	)
	flag.StringVar(&addr, "addr", ":8080", "listen address")
	flag.StringVar(&storagePath, "storage", "memory", "storage backend (memory or s3://endpoint/bucket)")
	flag.Parse()

	store := createStore(storagePath)
	srv := gateway.NewServer(store)

	log.Printf("artifact-gateway listening on %s (storage=%s)", addr, storagePath)
	log.Fatal(http.ListenAndServe(addr, srv))
}

// createStore builds a Store from the storage flag value.
// Supported formats:
//
//	"memory"                       — in-memory store (for testing)
//	"s3://endpoint/bucket"         — S3-compatible store
func createStore(storagePath string) artifact.Store {
	if storagePath == "memory" || storagePath == "" {
		log.Println("using in-memory artifact store")
		return artifact.NewMemoryStore()
	}
	if strings.HasPrefix(storagePath, "s3://") {
		trimmed := strings.TrimPrefix(storagePath, "s3://")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			log.Fatalf("invalid s3 storage path %q: expected s3://endpoint/bucket", storagePath)
		}
		endpoint := "https://" + parts[0]
		bucket := parts[1]
		log.Printf("using S3 artifact store: endpoint=%s bucket=%s", endpoint, bucket)
		return artifact.NewS3Store(endpoint, bucket, "", "")
	}
	log.Fatalf("unsupported storage backend: %s", storagePath)
	return nil
}
