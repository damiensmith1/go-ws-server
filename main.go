// Command go-ws-server runs the WebSocket server.
//
// Configuration is read from environment variables; see .env.example for
// the full list. The server is stateless — every running instance shares
// state through Redis, so you can run as many copies behind a TCP/HTTP
// load balancer as your traffic warrants.
//
// This command is deliberately a thin wrapper over package wsserver. To
// embed the server in another program, or to supply an auth.Verifier or
// authz.Authorizer in code, import that package directly.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/damiensmith1/go-ws-server/internal/logger"
	"github.com/damiensmith1/go-ws-server/wsserver"
)

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func mainErr() error {
	opts, err := wsserver.OptionsFromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(os.Stdout, opts.LogLevel)
	slog.SetDefault(log)
	opts.Logger = log

	srv, err := wsserver.New(opts)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return srv.Run(ctx)
}
