// Command controlplane runs the scheduler, N simulated hosts and the web UI
// in one process. Phase 0 stub: serves /healthz on $PORT so the deploy
// pipeline can be built before the components exist.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := flag.String("addr", "", "listen address (default :$PORT or :8080)")
	flag.Parse()
	if *addr == "" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		*addr = ":" + port
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	log.Printf("controlplane listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
