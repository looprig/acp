package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr))
}

// run is main's entire body, factored out so exampleagent_test.go can drive
// it in-process too (mirroring acp/internal/mockpeer/main.go's identical
// split), though the golden probes in this package spawn a real built binary
// as a subprocess — the same way a human pointing Zed at this binary would
// run it.
func run(stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	host := NewHost()
	a, err := agent.New(agent.Options{
		Host:     host,
		Replayer: host,
		Catalog:  host,
	})
	if err != nil {
		fmt.Fprintf(stderr, "exampleagent: agent.New: %v\n", err)
		return 1
	}

	conn := protocol.NewConn(stdin, stdout, protocol.ConnOptions{})
	a.Register(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := stdio.Serve(ctx, stdin, stdout, conn); err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "exampleagent: serve: %v\n", err)
		return 1
	}
	return 0
}
