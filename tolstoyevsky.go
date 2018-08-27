package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
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
}

var logger zerolog.Logger

var semicolon = []byte{';'}

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
		Msg("Starting Tolstoyevsky")

	router := fasthttprouter.New()
	router.GET("/stories/:story", readStoryHandler)

	if err := fasthttp.ListenAndServe(args.ListenAddr, router.Handler); err != nil {
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
	flag.Parse()
}

func initLogger() {
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	if args.PrettyLog {
		logger = logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

func redisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", args.RedisAddr, redis.DialReadTimeout(args.ReadTimeout))
}

type StoryError struct {
	Msg   string
	Story string
	Code  uint16
	Err   *error
}

type StoryReadCtx struct {
	RedisConn      redis.Conn
	KeyPrefix      string
	XReadCount     uint
	EntriesToFlush uint64
	Story          string
	ConnId         uint64
}

type Anchor struct {
	Stream  []byte
	FirstId string
	LastId  []byte
}

const RedisConnErr = "failed to create connection to Redis"
const NoAnchorsErr = "failed to load anchors for the story"

func (e StoryError) output(ctx *fasthttp.RequestCtx) {
	errId := uuid.Must(uuid.NewV4()).String()
	logger.Error().
		Err(*e.Err).
		Str("story", e.Story).
		Uint64("connId", ctx.ConnID()).
		Str("errId", errId).
		Uint16("code", e.Code).
		Msg(e.Msg)
	ctx.Error(
		fmt.Sprintf(`{"type":"error","code":%d,"msg":%q,"cause":%q,"id":%q}`,
			e.Code, e.Msg, e.Err, errId),
		500,
	)
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

func readStory(story string, ctx *fasthttp.RequestCtx) {
	if redisConn, err := redisConnection(); err == nil {
		srCtx := StoryReadCtx{
			RedisConn:      redisConn,
			KeyPrefix:      args.KeyPrefix,
			XReadCount:     args.XReadCount,
			ConnId:         ctx.ConnID(),
			EntriesToFlush: args.EntriesToFlush,
			Story:          story,
		}
		anchors, err := srCtx.loadAnchors(story)
		if err == nil {
			ctx.SetBodyStreamWriter(func(writer *bufio.Writer) {
				srCtx.pump(anchors, writer)
			})
		} else {
			StoryError{Msg: NoAnchorsErr, Story: story, Code: 2, Err: &err}.output(ctx)
		}
	} else {
		StoryError{Msg: RedisConnErr, Story: story, Code: 1, Err: &err}.output(ctx)
	}
}

func parseAnchor(anchor []byte) Anchor {
	stream := anchor[:bytes.Index(anchor, semicolon)]
	trimmed := anchor[len(stream)+1:]
	firstId := string(trimmed[:bytes.Index(trimmed, semicolon)])
	lastId := trimmed[len(firstId)+1:]
	return Anchor{Stream: stream, FirstId: firstId, LastId: lastId}
}

func (ctx StoryReadCtx) loadAnchors(story string) ([]Anchor, error) {
	result, err := ctx.RedisConn.Do("LRANGE", ctx.KeyPrefix+"story:"+story, 0, -1)
	if err == nil {
		anchors := make([]Anchor, len(result.([]interface{})))
		for i, a := range result.([]interface{}) {
			anchors[i] = parseAnchor(a.([]byte))
		}
		return anchors, nil
	} else {
		return nil, err
	}
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

func (ctx StoryReadCtx) pump(anchors []Anchor, writer *bufio.Writer) {
	var entriesWritten uint64 = 0
	for _, anchor := range anchors {
		firstId := anchor.FirstId
		for {
			xReadArgs := []interface{}{
				"COUNT", ctx.XReadCount, "BLOCK", 0, "STREAMS", anchor.Stream, firstId,
			}
			batch, err := ctx.RedisConn.Do("XREAD", xReadArgs...)
			if batch == nil {
				// this guard is useful in test with incorrectly mocked redis
				// TODO logging and writer output
			} else if err == nil {
				entries := toEntries(batch)
				for _, entry := range entries {
					writeEntry(entry, writer, anchor.Stream)
					if ctx.EntriesToFlush != 0 {
						entriesWritten++
						if entriesWritten%ctx.EntriesToFlush == 0 {
							logger.Debug().
								Uint64("entriesWritten", entriesWritten).
								Uint64("connId", ctx.ConnId).
								Str("story", ctx.Story).
								Bytes("stream", anchor.Stream).
								Uint64("entriesToFlush", ctx.EntriesToFlush).
								Msg("Flushing entries writer buffer")
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
				// TODO
				return
			}
		}
	}
}
