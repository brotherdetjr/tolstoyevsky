package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/buaazp/fasthttprouter"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"
	"github.com/valyala/fasthttp"
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

var semicolon = []byte{';'}

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

	var shutdownCh = make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGTERM)
	signal.Notify(shutdownCh, syscall.SIGINT)
	go func() {
		for sig := range shutdownCh {
			// Will appear soon, hopefully. See https://github.com/valyala/fasthttp/commit/e3d61d58
			// s.Shutdown()
			contexts.Range(func(key, value interface{}) bool {
				// TODO use INFO level
				value.(*StoryReadCtx).writeError(errors.New(sig.String()), "Shutting down")
				return true
			})
			time.Sleep(2 * time.Second)
			os.Exit(0)
		}
	}()

	if err := httpServer.ListenAndServe(args.ListenAddr); err != nil {
		logger.Fatal().
			Err(err).
			Msg("Error in ListenAndServe")
	}
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
	args.Debug = *flag.Bool("debug", false, "sets log level to debug")
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

type Anchor struct {
	Stream  []byte
	FirstId string
	LastId  []byte
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
		anchors, err := ctx.loadAnchors(story)
		if err == nil {
			httpCtx.SetBodyStreamWriter(func(writer *bufio.Writer) {
				ctx.HttpWriter = writer
				ctx.pump(anchors, writer)
			})
		} else {
			ctx.writeError(err, "failed to load anchors for the story")
		}
	} else {
		ctx := StoryReadCtx{HttpCtx: httpCtx, Story: story}
		ctx.writeError(err, "failed to create connection to Redis")
	}
}

func parseAnchor(anchor []byte) Anchor {
	stream := anchor[:bytes.Index(anchor, semicolon)]
	trimmed := anchor[len(stream)+1:]
	firstId := string(trimmed[:bytes.Index(trimmed, semicolon)])
	lastId := trimmed[len(firstId)+1:]
	return Anchor{Stream: stream, FirstId: firstId, LastId: lastId}
}

func toEntries(result interface{}) []interface{} {
	return result.([]interface{})[0].([]interface{})[1].([]interface{})
}

func strByteCmp(a string, b []byte) bool {
	abp := *(*[]byte)(unsafe.Pointer(&a))
	return bytes.Equal(abp, b)
}

func isLastBatch(batch []interface{}, lastId []byte) bool {
	for _, entry := range batch {
		if strByteCmp(entry.([]interface{})[0].(string), lastId) {
			return true
		}
	}
	return false
}

func lastIdInBatch(batch []interface{}) string {
	return batch[len(batch)-1].([]interface{})[0].(string)
}

func writeEntry(entry interface{}, writer *bufio.Writer, stream []byte) {
	writer.WriteString(`{"type":"event","id":"`)
	writer.Write(stream)
	writer.WriteString("-")
	writer.WriteString(entry.([]interface{})[0].(string))
	writer.WriteString(`","payload":`)
	writer.Write(entry.([]interface{})[1].([]interface{})[1].([]byte))
	writer.WriteString("}")
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
	msg.WriteByte('}')
	log.Msg(description)
	if ctx.HttpWriter != nil {
		ctx.HttpWriter.WriteString(msg.String())
		ctx.HttpWriter.Flush()
	} else {
		ctx.HttpCtx.Error(msg.String(), 500)
	}
}

func (ctx *StoryReadCtx) loadAnchors(story string) ([]Anchor, error) {
	result, err := ctx.RedisConn.Do("LRANGE", ctx.KeyPrefix+"story:"+story, 0, -1)
	if err == nil {
		if len(result.([]interface{})) == 0 {
			return nil, errors.New("no anchors")
		} else {
			anchors := make([]Anchor, len(result.([]interface{})))
			for i, a := range result.([]interface{}) {
				anchors[i] = parseAnchor(a.([]byte))
			}
			return anchors, nil
		}
	} else {
		return nil, err
	}
}

func (ctx *StoryReadCtx) pump(anchors []Anchor, writer *bufio.Writer) {
	var entriesWritten uint64 = 0
	for _, anchor := range anchors {
		firstId := anchor.FirstId
		for {
			xReadArgs := []interface{}{
				"COUNT", ctx.XReadCount, "BLOCK", 0, "STREAMS", anchor.Stream, firstId,
			}
			batch, err := ctx.RedisConn.Do("XREAD", xReadArgs...)
			if batch != nil && err == nil {
				entries := toEntries(batch)
				for _, entry := range entries {
					writeEntry(entry, writer, anchor.Stream)
					if ctx.EntriesToFlush != 0 {
						entriesWritten++
						if entriesWritten%ctx.EntriesToFlush == 0 {
							ctx.logFlush(entriesWritten, anchor)
							// TODO how does oboe.js treat trimmed json, when buffer is filled and flushed?
							writer.Flush()
						}
					}
				}
				if isLastBatch(entries, anchor.LastId) {
					break
				} else {
					firstId = lastIdInBatch(entries)
				}
			} else {
				ctx.writeError(err, "failed to XREAD", "stream", string(anchor.Stream))
				return
			}
		}
	}
}

func (ctx *StoryReadCtx) logFlush(entriesWritten uint64, anchor Anchor) {
	logger.Debug().
		Uint64("entriesWritten", entriesWritten).
		Uint64("connId", ctx.HttpCtx.ConnID()).
		Str("story", ctx.Story).
		Bytes("stream", anchor.Stream).
		Uint64("entriesToFlush", ctx.EntriesToFlush).
		Msg("Flushing entries writer buffer")
}
