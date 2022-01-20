package main

import (
	"log"

	"github.com/AYM1607/proglog/prototype/internal/server"
)

func main() {
	srv := server.NewHTPPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
