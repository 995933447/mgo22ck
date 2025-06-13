package main

import (
	"github.com/995933447/goconsole"
	"github.com/995933447/log-go/v2/loggo"
	"github.com/995933447/std-go/scan"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	goconsole.Register("start", "-c $configFilePath", func() {
		confFilePath := scan.OptStrDefault("c", "")

		if err := start(confFilePath); err != nil {
			panic(err)
		}

		defer onExit()

		loggo.Info("start success...")

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGUSR1, syscall.SIGTERM)
		for {
			sig := <-sigChan
			loggo.Infof("receive signal：%d", sig)
			switch sig {
			case syscall.SIGUSR1:
				loggo.Info("reload config...")
				if err := reload(confFilePath); err != nil {
					panic(err)
				}
				loggo.Info("reload config success...")
			case syscall.SIGTERM:
				loggo.Info("graceful exit...")
				stop()
				loggo.Info("graceful exit success...")
				os.Exit(0)
			}
		}
	})

	goconsole.Run()
}
