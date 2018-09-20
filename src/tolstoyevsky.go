package main

import (
	"./tolstoyevsky"
	"flag"
	"github.com/rs/zerolog"
	"os"
	"time"
)

func main() {
	args := parseArgs()
	tolstoyevsky.New(args, createLogger(args)).AwaitShutdown().Close()
}

func parseArgs() tolstoyevsky.Args {
	listenAddr := flag.String("listenAddr", ":8080", "TCP address to listen to")
	redisAddr := flag.String("redisAddr", ":6379", "Redis address:port")
	readTimeout := flag.Duration("readTimeout", 24*time.Hour, "Redis read timeout")
	keyPrefix := flag.String("keyPrefix", "tolstoyevsky:", "Redis key prefix to avoid name clashing")
	prettyLog := flag.Bool("prettyLog", true, "Outputs the log prettily printed and colored (slower)")
	xReadCount := flag.Uint("xReadCount", 512, "XREAD COUNT value")
	entriesToFlush := flag.Uint64("entriesToFlush", 1, "Entries count to flush the writer after. "+
		"If 0, flush policy is determined by buffer capacity")
	debug := flag.Bool("debug", false, "Sets log level to debug")

	flag.Parse()

	var args tolstoyevsky.Args
	args.ListenAddr = *listenAddr
	args.RedisAddr = *redisAddr
	args.ReadTimeout = *readTimeout
	args.KeyPrefix = *keyPrefix
	args.PrettyLog = *prettyLog
	args.XReadCount = *xReadCount
	args.EntriesToFlush = *entriesToFlush
	args.Debug = *debug
	return args
}

func createLogger(args tolstoyevsky.Args) *zerolog.Logger {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	if args.PrettyLog {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if args.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	return &logger
}
