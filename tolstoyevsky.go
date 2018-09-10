package main

import (
	"bufio"
	"flag"
	"github.com/buaazp/fasthttprouter"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"
	"github.com/valyala/fasthttp"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var args struct {
	ListenAddr     string
	RedisAddr      string
	ReadTimeout    time.Duration
	KeyPrefix      string
	PrettyLog      bool
	XReadCount     uint
	EntriesToFlush uint64
	Debug          bool
}

var logger zerolog.Logger

var contexts sync.Map

func main() {
	parseArgs()
	initLogger()

	logger.Info().
		Str("listenAddr", args.ListenAddr).
		Str("redisAddr", args.RedisAddr).
		Dur("readTimeout", args.ReadTimeout).
		Str("keyPrefix", args.KeyPrefix).
		Bool("prettyLog", args.PrettyLog).
		Uint("xReadCount", args.XReadCount).
		Uint64("entriesToFlush", args.EntriesToFlush).
		Bool("debug", args.Debug).
		Msg("Starting Tolstoyevsky")

	router := fasthttprouter.New()
	router.GET("/stories/:story", readStoryHandler)

	httpServer := &fasthttp.Server{Handler: router.Handler}

	go func() {
		if err := httpServer.ListenAndServe(args.ListenAddr); err != nil {
			logger.Fatal().
				Err(err).
				Msg("Error in ListenAndServe")
		}
	}()

	handleShutdown(httpServer)

}

func handleShutdown(httpServer *fasthttp.Server) {
	var shutdownCh = make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGTERM)
	signal.Notify(shutdownCh, syscall.SIGINT)
	sig := <-shutdownCh
	logger.Info().Msg("Shutting down")
	contexts.Range(func(key, value interface{}) bool {
		ctx := value.(*StoryReadCtx)
		ctx.writeInfo("Closing connection", "signal", sig.String())
		ctx.HttpWriter.Flush()
		ctx.HttpCtx.ResetBody()
		return true
	})
	httpServer.Shutdown()
}

func parseArgs() {
	args.ListenAddr = *flag.String("listenAddr", ":8080", "TCP address to listen to")
	args.RedisAddr = *flag.String("redisAddr", ":6379", "Redis address:port")
	args.ReadTimeout = *flag.Duration("readTimeout", 24*time.Hour, "Redis read timeout")
	args.KeyPrefix = *flag.String("keyPrefix", "tolstoyevsky:", "Redis key prefix to avoid name clashing")
	args.PrettyLog = *flag.Bool("prettyLog", true, "Outputs the log prettily printed and colored (slower)")
	args.XReadCount = *flag.Uint("xReadCount", 512, "XREAD COUNT value")
	args.EntriesToFlush = *flag.Uint64("entriesToFlush", 1, "Entries count to flush the writer after. "+
		"If 0, flush policy is determined by buffer capacity")
	args.Debug = *flag.Bool("debug", false, "Sets log level to debug")
	flag.Parse()
}

func initLogger() {
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	if args.PrettyLog {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if args.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
}

func redisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", args.RedisAddr, redis.DialReadTimeout(args.ReadTimeout))
}

type HttpContext interface {
	SetBodyStreamWriter(sw fasthttp.StreamWriter)
	ConnID() uint64
	Error(msg string, statusCode int)
	ResetBody()
}

type StoryReadCtx struct {
	RedisConn      redis.Conn
	KeyPrefix      string
	XReadCount     uint
	EntriesToFlush uint64
	Story          string
	HttpCtx        HttpContext
	HttpWriter     *bufio.Writer
}

type Patch struct {
	FirstEntryId string
	LastEntryId  string
	Id           string
}

func readStoryHandler(ctx *fasthttp.RequestCtx) {
	story := ctx.UserValue("story").(string)
	logger.Info().
		Str("story", story).
		Str("remoteAddr", ctx.RemoteIP().String()).
		Uint64("connId", ctx.ConnID()).
		Msg("Reading story")
	ctx.SetContentType("application/stream+json; charset=utf8")
	readStory(story, ctx)
}

func readStory(story string, httpCtx *fasthttp.RequestCtx) {
	if redisConn, err := redisConnection(); err == nil {
		ctx := StoryReadCtx{
			RedisConn:      redisConn,
			KeyPrefix:      args.KeyPrefix,
			XReadCount:     args.XReadCount,
			HttpCtx:        httpCtx,
			EntriesToFlush: args.EntriesToFlush,
			Story:          story,
		}
		contexts.Store(httpCtx.ConnID(), &ctx)
		if err == nil {
			httpCtx.SetBodyStreamWriter(func(writer *bufio.Writer) {
				ctx.HttpWriter = writer
				ctx.pump([]Patch{}, writer)
			})
		} else {
			ctx.writeError(err, "failed to load patches for the story")
		}
	} else {
		ctx := StoryReadCtx{HttpCtx: httpCtx, Story: story}
		ctx.writeError(err, "failed to create connection to Redis")
	}
}

func toEntries(result interface{}) []interface{} {
	return result.([]interface{})[0].([]interface{})[1].([]interface{})
}

func lastIdInBatch(batch []interface{}) string {
	return batch[len(batch)-1].([]interface{})[0].(string)
}

func writeEntry(entry interface{}, writer *bufio.Writer, key string) {
	writer.WriteString(`{"type":"event","id":"`)
	writer.WriteString(key)
	writer.WriteString("-")
	writer.WriteString(entry.([]interface{})[0].(string))
	writer.WriteString(`","payload":`)
	writer.Write(entry.([]interface{})[1].([]interface{})[1].([]byte))
	writer.WriteString("}")
}

type KeyValueWriter interface {
	Str(k, v string) *zerolog.Event
}

func writeKeyValues(msg *strings.Builder, log KeyValueWriter, keyValues ...string) {
	var k string
	for i, kv := range keyValues {
		if i%2 == 0 {
			k = kv
		} else {
			log.Str(k, kv)
			msg.WriteString(`,"`)
			msg.WriteString(k)
			msg.WriteString(`":"`)
			msg.WriteString(kv)
			msg.WriteByte('"')
		}
	}
}

func (ctx *StoryReadCtx) writeInfo(description string, keyValues ...string) {
	var log = logger.Info().
		Str("story", ctx.Story).
		Uint64("connId", ctx.HttpCtx.ConnID())
	var msg strings.Builder
	msg.WriteString(`{"type":"info","msg":"`)
	msg.WriteString(description)
	msg.WriteByte('"')
	writeKeyValues(&msg, log, keyValues...)
	msg.WriteByte('}')
	log.Msg(description)
	if ctx.HttpWriter != nil {
		ctx.HttpWriter.WriteString(msg.String())
	}
}

func (ctx *StoryReadCtx) writeError(err error, description string, keyValues ...string) {
	errId := uuid.Must(uuid.NewV4()).String()
	var log = logger.Error().
		Err(err).
		Str("story", ctx.Story).
		Uint64("connId", ctx.HttpCtx.ConnID()).
		Str("errId", errId)
	var msg strings.Builder
	msg.WriteString(`{"type":"error","msg":"`)
	msg.WriteString(description)
	msg.WriteString(`","cause":`)
	msg.WriteString(strconv.Quote(err.Error()))
	msg.WriteString(`,"id":"`)
	msg.WriteString(errId)
	msg.WriteByte('"')
	writeKeyValues(&msg, log, keyValues...)
	msg.WriteByte('}')
	log.Msg(description)
	if ctx.HttpWriter != nil {
		ctx.HttpWriter.WriteString(msg.String())
	} else {
		ctx.HttpCtx.Error(msg.String(), 500)
	}
}

func (ctx *StoryReadCtx) pump(patches []Patch, writer *bufio.Writer) {
	var entriesWritten uint64 = 0
	key := ctx.KeyPrefix + "stories:" + ctx.Story
	firstId := "0"
	for {
		xReadArgs := []interface{}{
			"COUNT", ctx.XReadCount, "BLOCK", 0, "STREAMS", key, firstId,
		}
		batch, err := ctx.RedisConn.Do("XREAD", xReadArgs...)
		if batch != nil && err == nil {
			entries := toEntries(batch)
			for _, entry := range entries {
				writeEntry(entry, writer, key)
				if ctx.EntriesToFlush != 0 {
					entriesWritten++
					if entriesWritten%ctx.EntriesToFlush == 0 {
						ctx.logFlush(entriesWritten)
						// TODO how does oboe.js treat trimmed json, when buffer is filled and flushed?
						writer.Flush()
					}
				}
			}
			firstId = lastIdInBatch(entries)
		} else if err != nil {
			ctx.writeError(err, "failed to XREAD", "stream", key)
			writer.Flush()
			return
		} else {
			break
		}
	}
}

func (ctx *StoryReadCtx) logFlush(entriesWritten uint64) {
	logger.Debug().
		Uint64("entriesWritten", entriesWritten).
		Uint64("connId", ctx.HttpCtx.ConnID()).
		Str("story", ctx.Story).
		Uint64("entriesToFlush", ctx.EntriesToFlush).
		Msg("Flushing entries writer buffer")
}
