// Command go-ws-server runs the WebSocket server.
//
// Configuration is read from environment variables; see .env.example for
// the full list. The server is stateless — every running instance shares
// state through Redis, so you can run as many copies behind a TCP/HTTP
// load balancer as your traffic warrants.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/damiensmith1/go-ws-server/internal/app"
	"github.com/damiensmith1/go-ws-server/internal/config"
	"github.com/damiensmith1/go-ws-server/internal/logger"
)

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func mainErr() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(os.Stdout, cfg.LogLevel)
	slog.SetDefault(log)

	a, err := app.New(cfg, log)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return a.Run(ctx)
}
