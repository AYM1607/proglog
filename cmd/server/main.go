package main

import (
	"log"

	"github.com/AYM1607/proglog/old/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
