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
	ListenAddr  string
	RedisAddr   string
	ReadTimeout time.Duration
	KeyPrefix   string
	PrettyLog   bool
	XReadCount  uint
	// TODO OutputBufSize int
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
	flag.Parse()
	args.ListenAddr = *flag.String("listenAddr", ":8080", "TCP address to listen to")
	args.RedisAddr = *flag.String("redisAddr", ":6379", "Redis address:port")
	args.ReadTimeout = *flag.Duration("readTimeout", 24*time.Hour, "Redis read timeout")
	args.KeyPrefix = *flag.String("keyPrefix", "tolstoyevsky:", "Redis key prefix to avoid name clashing")
	args.PrettyLog = *flag.Bool("prettyLog", true, "Outputs the log prettily printed and colored (slower)")
	args.XReadCount = *flag.Uint("xReadCount", 512, "XREAD COUNT value")
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

type Redis struct {
	Conn       redis.Conn
	KeyPrefix  string
	XReadCount uint
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
		rds := Redis{Conn: redisConn, KeyPrefix: args.KeyPrefix, XReadCount: args.XReadCount}
		anchors, err := rds.loadAnchors(story)
		if err == nil {
			ctx.SetBodyStreamWriter(func(writer *bufio.Writer) {
				rds.pump(anchors, writer)
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

func (redis Redis) loadAnchors(story string) ([]Anchor, error) {
	result, err := redis.Conn.Do("LRANGE", redis.KeyPrefix+"story:"+story, 0, -1)
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

func toWriter(writer *bufio.Writer, stream []byte, id string, payload []byte) {
	writer.WriteString(`{"type":"event","id":"`)
	writer.Write(stream)
	writer.WriteString("-")
	writer.WriteString(id)
	writer.WriteString(`","payload":`)
	writer.Write(payload)
	writer.WriteString("}")
}

func (redis Redis) pump(anchors []Anchor, writer *bufio.Writer) {
	for _, anchor := range anchors {
		firstId := anchor.FirstId
		for {
			xReadArgs := []interface{}{
				"COUNT", redis.XReadCount, "BLOCK", 0, "STREAMS", anchor.Stream, firstId,
			}
			batch, err := redis.Conn.Do("XREAD", xReadArgs...)
			if batch == nil {
				// this guard is useful in test with incorrectly mocked redis
				// TODO logging and writer output
			} else if err == nil {
				entries := toEntries(batch)
				for _, entry := range entries {
					e := entry.([]interface{})
					toWriter(
						writer,
						anchor.Stream,
						e[0].(string),
						e[1].([]interface{})[1].([]byte),
					)
				}
				// TODO how does oboe.js treat trimmed json, when buffer is filled and flushed?
				writer.Flush()
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
