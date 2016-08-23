package graphitesender

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"
	"regexp"
	"fmt"
)

const (
	packetMaxLen = 1400
	flushTime = 10 * time.Second
)

var (
	merticPattern = regexp.MustCompile("^[a-zA-Z0-9_-]+\\.[a-zA-Z0-9_-]+\\.\\S+\\s+[-0-9.eE+]+\\s+\\d{8,10}\\n$")
	//MaxMetrics = 50000
	MaxMetrics = 50
)

type Sender struct {
	mu        *sync.Mutex
	collector []string
	n int
	buf       bytes.Buffer
	wr        io.Writer
}

func NewSender(w io.Writer) *Sender {
	//addr, err := net.ResolveTCPAddr("tcp", "localhost:42033")
	//if err != nil {
	//	return err
	//}
	//conn, err := net.DialTCP("tcp", nil, addr)
	//if err != nil {
	//	return err
	//}
	//defer conn.Close()

	sender := &Sender{
		mu:        &sync.Mutex{},
		collector: make([]string, 0, MaxMetrics),
		wr:        w,
	}
	go sender.run()
	return sender
}

func (s *Sender) WriteMetric(m string) error {
	if !merticPattern.MatchString(m) {
		return fmt.Errorf("bad metric: %s", s)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("new metric: %s", m)

	s.collector = append(s.collector, m)
	if len(s.collector) == MaxMetrics {
		log.Println("collector is packed")
		//	if err := s.Flush(); err != nil {
		//		return err
		//	}
	}
	return nil
}

func (s *Sender) Flush() error {
	s.mu.Lock()

	n := len(s.collector)
	if n - s.n > MaxMetrics {
		n = MaxMetrics
	}
	collector := s.collector[s.n:n]
	s.n += n
	// TODO: check if this' safe way to reset a slice
	//s.collector = s.collector[n:]

	log.Printf("flushing: took %d, left %d", len(collector), len(s.collector) - s.n)

	s.mu.Unlock()

	if len(collector) == 0 {
		log.Println("nothing to flush")
		return nil
	}

	log.Println("flushing collected metrics")

	var err error

	for _, metric := range collector {
		if len(metric) + s.buf.Len() > packetMaxLen {
			log.Println("buffer is packed; flush earlier")
			err := s.flush()
			if err != nil {
				log.Printf("error flushing: %v", err)
			}
		}
		s.buf.WriteString(metric)
	}

	err = s.flush()
	if err != nil {
		return err
	}

	return nil
}

func (s *Sender) flush() error {
	if s.buf.Len() > 0 {
		_, err := s.wr.Write(s.buf.Bytes())
		s.buf.Reset()
		return err
	}
	return nil
}

func (s *Sender) run() {
	flushTicker := time.NewTicker(10 * time.Second)
	defer flushTicker.Stop()

	sysTicker := time.NewTicker(60 * time.Second)
	defer sysTicker.Stop()

	for {
		select {
		case <-sysTicker.C:
			s.mu.Lock()
			dropped := len(s.collector) - s.n
			log.Printf("dropping %d old metrics\n", dropped)
			s.collector = s.collector[s.n:]
			s.n = 0
			s.mu.Unlock()

		case <-flushTicker.C:
			err := s.Flush()
			if err != nil {
				log.Printf("error flushing: %v", err)
			}
		}
	}
}
