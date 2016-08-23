package main

import (
	"io"
	"bufio"
	"os"
	"syscall"
	"net"
	"log"
	"os/signal"

	sender "github.com/narqo/graphite-sender"
)

var (
	globalSender *sender.Sender
	graphiteAddress = ":42001"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	globalSender = sender.NewSender(os.Stdout)

	sock, err := net.Listen("tcp", graphiteAddress)
	if err != nil {
		log.Fatalf("error binding to TCP: %v", err)
	}
	log.Printf("bound to TCP: %s\n", graphiteAddress)
	go func() {
		for {
			conn, err := sock.Accept()
			if err != nil {
				log.Printf("error accepting connection: %v\n", err)
				continue
			}
			go func() {
				defer conn.Close()
				processReader(conn)
			}()
		}
	}()

	done := make(chan struct{}, 1)
	go func() {
		sig := <-sigs
		log.Println(sig)
		done <- struct{}{}
	}()

	log.Println("awaiting exit signal")
	<-done
}

func processReader(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for {
		if ok := scanner.Scan(); !ok {
			break
		}
		processLine(scanner.Text())
	}
}

func processLine(line string) {
	// TODO: could we drop string concatination here to reduce allocation?
	err := globalSender.WriteMetric(line + "\n")
	if err != nil {
		log.Printf("error collecting line: %v", err)
	}
}

