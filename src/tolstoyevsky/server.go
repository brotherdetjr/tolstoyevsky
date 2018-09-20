package tolstoyevsky

import (
	"bufio"
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


type Tolstoyevsky struct {
	Args       Args
	Logger     *zerolog.Logger
	Contexts   sync.Map
	HttpServer *fasthttp.Server
}

type Args struct {
	ListenAddr     string
	RedisAddr      string
	ReadTimeout    time.Duration
	KeyPrefix      string
	PrettyLog      bool
	XReadCount     uint
	EntriesToFlush uint64
	Debug          bool
}

func New(args Args, logger *zerolog.Logger) *Tolstoyevsky {

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
	httpServer := &fasthttp.Server{Handler: router.Handler}
	var t = Tolstoyevsky{Args: args, Logger: logger, HttpServer: httpServer}
	router.GET("/stories/:story", t.readStoryHandler())

	go func() {
		if err := httpServer.ListenAndServe(t.Args.ListenAddr); err != nil {
			t.Logger.Panic().
				Err(err).
				Msg("Error in ListenAndServe")
		}
	}()

	return &t
}

func (t *Tolstoyevsky) AwaitShutdown() *Tolstoyevsky {
	var shutdownCh = make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-shutdownCh
	t.Logger.Info().
		Str("signal", sig.String()).
		Msg("Shutting down")
	return t
}

func (t *Tolstoyevsky) Close() error {
	err := t.HttpServer.Shutdown()
	t.Contexts.Range(func(key, value interface{}) bool {
		ctx := value.(*storyReadCtx)
		ctx.writeInfo("Closing connection")
		ctx.HttpWriter.Flush()
		ctx.HttpCtx.ResetBody()
		return true
	})
	return err
}

// private

type httpContext interface {
	SetBodyStreamWriter(sw fasthttp.StreamWriter)
	ConnID() uint64
	Error(msg string, statusCode int)
	ResetBody()
}

type storyReadCtx struct {
	RedisConn      redis.Conn
	KeyPrefix      string
	XReadCount     uint
	EntriesToFlush uint64
	Story          string
	HttpCtx        httpContext
	HttpWriter     *bufio.Writer
	Contexts       *sync.Map
	Logger         *zerolog.Logger
}

func (t *Tolstoyevsky) redisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", t.Args.RedisAddr, redis.DialReadTimeout(t.Args.ReadTimeout))
}

func (t *Tolstoyevsky) readStoryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		story := ctx.UserValue("story").(string)
		t.Logger.Info().
			Str("story", story).
			Str("remoteAddr", ctx.RemoteIP().String()).
			Uint64("connId", ctx.ConnID()).
			Msg("Reading story")
		ctx.SetContentType("application/stream+json; charset=utf8")
		t.readStory(story, ctx)
	}
}

func (t *Tolstoyevsky) readStory(story string, httpCtx *fasthttp.RequestCtx) {
	if redisConn, err := t.redisConnection(); err == nil {
		ctx := storyReadCtx{
			RedisConn:      redisConn,
			KeyPrefix:      t.Args.KeyPrefix,
			XReadCount:     t.Args.XReadCount,
			HttpCtx:        httpCtx,
			EntriesToFlush: t.Args.EntriesToFlush,
			Story:          story,
			Contexts:       &(t.Contexts),
			Logger:         t.Logger,
		}
		t.Logger.Debug().
			Uint64("connId", ctx.HttpCtx.ConnID()).
			Msg("Registering new HTTP connection")
		t.Contexts.Store(ctx.HttpCtx.ConnID(), ctx)
		httpCtx.SetBodyStreamWriter(func(writer *bufio.Writer) {
			ctx.HttpWriter = writer
			ctx.pump(writer)
		})
	} else {
		ctx := storyReadCtx{HttpCtx: httpCtx, Story: story}
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

func writeKeyValues(msg *strings.Builder, log *zerolog.Event, keyValues ...string) {
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

func (ctx *storyReadCtx) writeInfo(description string, keyValues ...string) {
	var log = ctx.Logger.Info().
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

func (ctx *storyReadCtx) writeError(err error, description string, keyValues ...string) {
	errId := uuid.Must(uuid.NewV4()).String()
	var log = ctx.Logger.Error().
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

func (ctx *storyReadCtx) pump(writer *bufio.Writer) {
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
						ctx.Logger.Debug().
							Uint64("entriesWritten", entriesWritten).
							Uint64("connId", ctx.HttpCtx.ConnID()).
							Str("story", ctx.Story).
							Uint64("entriesToFlush", ctx.EntriesToFlush).
							Msg("Flushing entries writer buffer")
						// TODO how does oboe.js treat trimmed json, when buffer is filled and flushed?
						writer.Flush()
					}
				}
			}
			firstId = lastIdInBatch(entries)
		} else if err != nil {
			ctx.writeError(err, "failed to XREAD", "stream", key)
			writer.Flush()
			ctx.HttpCtx.ResetBody()
			ctx.Logger.Debug().
				Uint64("connId", ctx.HttpCtx.ConnID()).
				Msg("Unregistering HTTP connection")
			ctx.Contexts.Delete(ctx.HttpCtx.ConnID())
			return
		} else {
			break
		}
	}
}
