package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"forger/internal/config"
	"forger/internal/coordinator"
	"forger/internal/logx"
	"forger/internal/meta"
	"forger/internal/node"
)

func main() {
	cfg, err := config.Load(); if err != nil { log.Fatal(err) }
	logger := logx.New()
	logger.Info("starting forger", "mode", cfg.Mode, "listen", cfg.ListenAddr)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM); defer stop()
	var h http.Handler
	var closeFn func()
	if cfg.Mode == "node" { s,e:=node.New(cfg);if e!=nil{log.Fatal(e)};h=s.Handler() } else { db,e:=meta.Open(cfg.DatabasePath);if e!=nil{log.Fatal(e)};co:=coordinator.New(cfg,db);if e=co.Start(ctx);e!=nil{log.Fatal(e)};h=co.Handler();closeFn=func(){co.Stop();db.Close()} }
	srv:=&http.Server{Addr:cfg.ListenAddr,Handler:h};go func(){<-ctx.Done();srv.Shutdown(context.Background())}();err=srv.ListenAndServe();if closeFn!=nil{closeFn()};if err!=nil&&err!=http.ErrServerClosed{log.Fatal(err)}
}
