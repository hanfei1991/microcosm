package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hanfei1991/microcosom/executor"
	"github.com/hanfei1991/microcosom/pkg/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// 1. parse config
// 2. init logger
// 3. print log
// 4. start server
func main() {
	// 1. parse config
	cfg := executor.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		//common.PrintLinesf("parse cmd flags err: %s", terror.Message(err))
		os.Exit(2)
	}

	// 2. init logger
	err = log.InitLogger(&log.Config{
		File:   cfg.LogFile,
		Level:  strings.ToLower(cfg.LogLevel),
		Format: cfg.LogFormat,
	})
	if err != nil {
//		common.PrintLinesf("init logger error %s", terror.Message(err))
		os.Exit(2)
	}

	// 3. start server
	ctx, cancel := context.WithCancel(context.Background())
	server := executor.NewServer(cfg)
	if err != nil {
		log.L().Error("fail to start dm-master", zap.Error(err))
		os.Exit(2)
	}
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		cancel()
	}()
	err = server.Start(ctx)
	if err != nil {
		log.L().Error("fail to start dm-master", zap.Error(err))
		os.Exit(2)
	}
}