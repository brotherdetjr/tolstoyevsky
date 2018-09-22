package tolstoyevsky

import (
	"bufio"
	"github.com/buaazp/fasthttprouter"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
	"github.com/satori/go.uuid"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/reuseport"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Tolstoyevsky struct {
	Args          Args
	Logger        *zerolog.Logger
	Contexts      sync.Map
	HttpServer    *fasthttp.Server
	StoriesPrefix string
	RedisWrite    redis.Conn
	UuidSupplier  func() string
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
	var t = Tolstoyevsky{
		Args:          args,
		Logger:        logger,
		HttpServer:    httpServer,
		StoriesPrefix: args.KeyPrefix + "stories:",
		UuidSupplier:  UuidSupplier,
	}
	redisWrite, err := t.redisConnection()
	if err != nil {
		t.Logger.Panic().
			Err(err).
			Msg("Failed to create connection to Redis")
	}
	t.RedisWrite = redisWrite
	router.GET("/stories/:story", t.readStoryHandler())
	router.PUT("/stories/:story", t.writeStoryHandler())

	go func() {
		// reuseport allows linear scaling server performance on multi-CPU servers
		ln, err := reuseport.Listen("tcp4", t.Args.ListenAddr)
		if err != nil {
			t.Logger.Panic().
				Err(err).
				Msg("Error in reuseport.Listen")
		}
		if err = httpServer.Serve(ln); err != nil {
			t.Logger.Panic().
				Err(err).
				Msg("Error in httpServer.Serve")
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
	t.RedisWrite.Close()
	t.Contexts.Range(func(key, value interface{}) bool {
		ctx := value.(*storyReadCtx)
		ctx.writeShutdown()
		ctx.redisConn.Close()
		if ctx.httpWriter != nil {
			ctx.httpWriter.Flush()
		}
		ctx.httpCtx.ResetBody()
		return true
	})
	return t.HttpServer.Shutdown()
}

// Default implementation of UUID supplier function.
// Other implementations might be used in tests to
// have predictable UUID values.
func UuidSupplier() string {
	return uuid.Must(uuid.NewV4()).String()
}

// private

type httpContext interface {
	SetBodyStreamWriter(sw fasthttp.StreamWriter)
	ConnID() uint64
	Error(msg string, statusCode int)
	ResetBody()
}

type storyReadCtx struct {
	redisConn      redis.Conn
	keyPrefix      string
	xReadCount     uint
	entriesToFlush uint64
	story          string
	httpCtx        httpContext
	httpWriter     *bufio.Writer
	contexts       *sync.Map
	logger         *zerolog.Logger
	uuidSupplier   func() string
}

func (t *Tolstoyevsky) redisConnection() (redis.Conn, error) {
	return redis.Dial("tcp", t.Args.RedisAddr, redis.DialReadTimeout(t.Args.ReadTimeout))
}

func (t *Tolstoyevsky) writeStoryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		story := ctx.UserValue("story").(string)
		ctx.SetContentType("text/plain; charset=utf8")
		ctx.Response.Header.Set("Cache-Control", "no-cache")
		key := t.StoriesPrefix + story
		payload := ctx.Request.Body()
		newId, err := t.RedisWrite.Do("XADD", key, "*", "payload", payload)
		if err == nil {
			writeNewId(ctx, key, newId)
		} else {
			t.Logger.Warn().
				Err(err).
				Str("key", key).
				Msg("Failed to XADD. Trying to reconnect and XADD one more time")
			t.RedisWrite.Close()
			redisWrite, err := t.redisConnection()
			if err == nil {
				t.RedisWrite = redisWrite
				newId, err := redisWrite.Do("XADD", key, "*", "payload", payload)
				if err == nil {
					writeNewId(ctx, key, newId)
				} else {
					t.writePutError(ctx, "Failed to XADD", err, key, payload)
				}
			} else {
				t.writePutError(ctx, "Failed to reconnect to Redis", err, key, payload)
			}
		}
	}
}

func writeNewId(ctx *fasthttp.RequestCtx, key string, newId interface{}) {
	ctx.SetStatusCode(200)
	ctx.WriteString(key)
	ctx.WriteString("-")
	ctx.WriteString(newId.(string))
	ctx.WriteString("\n")
}

func (t *Tolstoyevsky) writePutError(
	ctx *fasthttp.RequestCtx, description string, err error, key string, payload []byte) {
	id := t.UuidSupplier()
	p := string(payload[:])
	t.Logger.Error().Err(err).
		Str("id", id).
		Str("key", key).
		Str("payload", p).
		Msg(description)
	ctx.SetStatusCode(500)
	ctx.WriteString(id)
	ctx.WriteString("\n")
	ctx.WriteString(description)
	ctx.WriteString("\n")
	ctx.WriteString(err.Error())
	ctx.WriteString("\n")
}

func (t *Tolstoyevsky) readStoryHandler() func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		story := ctx.UserValue("story").(string)
		t.Logger.Info().
			Str("story", story).
			Str("remoteAddr", ctx.RemoteIP().String()).
			Uint64("connId", ctx.ConnID()).
			Msg("Reading story")
		ctx.SetContentType("text/event-stream; charset=utf8")
		ctx.Response.Header.Set("Cache-Control", "no-cache")
		t.readStory(story, ctx)
	}
}

func (t *Tolstoyevsky) readStory(story string, httpCtx *fasthttp.RequestCtx) {
	if redisConn, err := t.redisConnection(); err == nil {
		ctx := storyReadCtx{
			redisConn:      redisConn,
			keyPrefix:      t.Args.KeyPrefix,
			xReadCount:     t.Args.XReadCount,
			httpCtx:        httpCtx,
			entriesToFlush: t.Args.EntriesToFlush,
			story:          story,
			contexts:       &(t.Contexts),
			logger:         t.Logger,
			uuidSupplier:   t.UuidSupplier,
		}
		t.Logger.Debug().
			Uint64("connId", ctx.httpCtx.ConnID()).
			Msg("Registering new HTTP connection for story reading")
		t.Contexts.Store(ctx.httpCtx.ConnID(), &ctx)
		httpCtx.SetBodyStreamWriter(func(writer *bufio.Writer) {
			ctx.httpWriter = writer
			ctx.pump(writer)
		})
	} else {
		ctx := storyReadCtx{
			httpCtx:      httpCtx,
			story:        story,
			logger:       t.Logger,
			uuidSupplier: t.UuidSupplier,
		}
		ctx.writeGetError(err, "Failed to create connection to Redis")
	}
}

func toEntries(result interface{}) []interface{} {
	return result.([]interface{})[0].([]interface{})[1].([]interface{})
}

func lastIdInBatch(batch []interface{}) string {
	return batch[len(batch)-1].([]interface{})[0].(string)
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

// The following three methods are quite ugly. A single
// common function writeEvent(id, event, data string) could be
// extracted. Though I sacrifice the readability here
// in order to avoid memory allocations. Otherwise we would
// need to do some string concatenation when forming the
// argument list for the writeEvent function, causing memory
// allocation.
func (ctx *storyReadCtx) writeShutdown() {
	var log = ctx.logger.Info().
		Str("story", ctx.story).
		Uint64("connId", ctx.httpCtx.ConnID())
	var msg strings.Builder
	msgId := ctx.uuidSupplier()
	msg.WriteString("id: ")
	msg.WriteString(msgId)
	msg.WriteString("\nevent: shutdown\ndata: 0\n\n")
	log.Msg("Server shutting down. Closing connection")
	if ctx.httpWriter != nil {
		ctx.httpWriter.WriteString(msg.String())
	}
}

func (ctx *storyReadCtx) writeGetError(err error, description string, keyValues ...string) {
	errId := ctx.uuidSupplier()
	var log = ctx.logger.Error().
		Err(err).
		Str("story", ctx.story).
		Uint64("connId", ctx.httpCtx.ConnID()).
		Str("errId", errId)
	var msg strings.Builder
	msg.WriteString("id: ")
	msg.WriteString(errId)
	msg.WriteString("\nevent: ")
	msg.WriteString("error")
	msg.WriteByte('\n')
	msg.WriteString(`data: {"description":"`)
	msg.WriteString(description)
	msg.WriteString(`","cause":`)
	msg.WriteString(strconv.Quote(err.Error()))
	writeKeyValues(&msg, log, keyValues...)
	msg.WriteString("}\n\n")
	log.Msg(description)
	if ctx.httpWriter != nil {
		ctx.httpWriter.WriteString(msg.String())
	} else {
		ctx.httpCtx.Error(msg.String(), 500)
	}
}

func writeEntry(entry interface{}, writer *bufio.Writer, key string) {
	writer.WriteString("id: ")
	writer.WriteString(key)
	writer.WriteString("-")
	writer.WriteString(entry.([]interface{})[0].(string))
	writer.WriteString("\nevent: ")
	writer.WriteString("entry")
	writer.WriteString("\ndata: ")
	writer.Write(entry.([]interface{})[1].([]interface{})[1].([]byte))
	writer.WriteByte('\n')
	writer.WriteByte('\n')
}

func (ctx *storyReadCtx) pump(writer *bufio.Writer) {
	var entriesWritten uint64 = 0
	key := ctx.keyPrefix + "stories:" + ctx.story
	firstId := "0"
	for {
		xReadArgs := []interface{}{
			"COUNT", ctx.xReadCount, "BLOCK", 0, "STREAMS", key, firstId,
		}
		batch, err := ctx.redisConn.Do("XREAD", xReadArgs...)
		if batch != nil && err == nil {
			entries := toEntries(batch)
			for _, entry := range entries {
				writeEntry(entry, writer, key)
				if ctx.entriesToFlush != 0 {
					entriesWritten++
					if entriesWritten%ctx.entriesToFlush == 0 {
						ctx.logger.Debug().
							Uint64("entriesWritten", entriesWritten).
							Uint64("connId", ctx.httpCtx.ConnID()).
							Str("story", ctx.story).
							Uint64("entriesToFlush", ctx.entriesToFlush).
							Msg("Flushing entries writer buffer")
						// TODO how does EventSource treat trimmed event, when buffer is filled and flushed?
						writer.Flush()
					}
				}
			}
			firstId = lastIdInBatch(entries)
		} else if err != nil {
			ctx.writeGetError(err, "Failed to XREAD", "stream", key)
			writer.Flush()
			ctx.httpCtx.ResetBody()
			ctx.logger.Debug().
				Uint64("connId", ctx.httpCtx.ConnID()).
				Msg("Unregistering HTTP connection")
			ctx.contexts.Delete(ctx.httpCtx.ConnID())
			return
		} else {
			break
		}
	}
}
