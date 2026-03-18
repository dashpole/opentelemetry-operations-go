package main

import (
	"fmt"
	"log"
	"os"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp"
)

func main() {
	detector := gcp.NewDetector()

	platform := detector.CloudPlatform()
	fmt.Printf("Detected Platform: %v\n", platform)

	if platform != gcp.CloudRunWorkerPool {
		fmt.Printf("Warning: Expected CloudRunWorkerPool (%v), got %v\n", gcp.CloudRunWorkerPool, platform)
	}

	// Dump environment variables for debugging
	fmt.Println("--- Environment Variables ---")
	for _, env := range []string{"CLOUD_RUN_WORKER_POOL", "CLOUD_RUN_WORKER_POOL_REVISION", "K_REVISION"} {
		fmt.Printf("%s: %s\n", env, os.Getenv(env))
	}
	fmt.Println("-----------------------------")

	// Verify the various FaaS methods
	name, err := detector.FaaSName()
	fmt.Printf("FaaSName: %q (err: %v)\n", name, err)

	version, err := detector.FaaSVersion()
	fmt.Printf("FaaSVersion: %q (err: %v)\n", version, err)

	id, err := detector.FaaSID()
	fmt.Printf("FaaSID: %q (err: %v)\n", id, err)

	region, err := detector.FaaSCloudRegion()
	fmt.Printf("FaaSCloudRegion: %q (err: %v)\n", region, err)

	log.Println("Finished detecting worker pool metadata.")
}
