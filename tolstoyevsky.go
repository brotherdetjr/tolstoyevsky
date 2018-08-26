package main

import (
    "flag"
    "time"
    "bufio"
    "fmt"
    "os"
    "strings"

    "github.com/valyala/fasthttp"
    "github.com/buaazp/fasthttprouter"
    "github.com/gomodule/redigo/redis"
    "github.com/satori/go.uuid"
    "github.com/rs/zerolog"
)

var args struct {
    ListenAddr string
    RedisAddr string
    ReadTimeout time.Duration
    KeyPrefix string
    PrettyLog bool
}

const initialId = "$"

var logger zerolog.Logger

func main() {
    parseArgs()
    initLogger()

    logger.Info().
        Str("listenAddr", args.ListenAddr).
        Str("redisAddr", args.RedisAddr).
        Dur("readTimeout", args.ReadTimeout).
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
    args.ReadTimeout = *flag.Duration("readTimeout", 24 * time.Hour, "Redis read timeout")
    args.KeyPrefix = *flag.String("keyPrefix", "tolstoyevsky:", "Redis key prefix to avoid name clashing")
    args.PrettyLog = *flag.Bool("prettyLog", true, "Outputs the log prettily printed and colored (slower)")
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
    Msg string
    Story string
    Code uint16
    Err *error
}

type Redis struct {
    Conn redis.Conn
    KeyPrefix string
    BatchSize uint32
}

type Anchor struct {
    Stream string
    FirstId string
    LastId string
}

var redisConnErr = "Failed to create connection to Redis"
var redisStreamNameErr = "Failed to retrieve stream name from Redis"

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
        readStoryConn(story, ctx, &redisConn)
    } else {
        StoryError{Msg: redisConnErr, Story: story, Code: 1, Err: &err}.output(ctx)
    }
}

func readStoryConn(story string, ctx *fasthttp.RequestCtx, redisConn *redis.Conn) {
    stream, err := redis.String((*redisConn).Do("LINDEX", args.KeyPrefix + "story:" + story, 0))
    if err == nil {
        ctx.SetBodyStreamWriter(newWriter(stream, ctx.ConnID(), redisConn, initialId))
    } else {
        StoryError{Msg: redisStreamNameErr, Story: story, Code: 2, Err: &err}.output(ctx)
    }
}

func newWriter(stream string, connId uint64, redisConn *redis.Conn, initialId string) func(writer *bufio.Writer) {
    return func(writer *bufio.Writer) {
        entryId := initialId
        for {
            result, err := (*redisConn).Do("XREAD", "COUNT", 0, "BLOCK", 0, "STREAMS", stream, entryId)
            if err == nil {
                data := result.([]interface {})[0].([]interface {})[1].([]interface {})
                for i, entry := range data {
                    chunkSize := len(data)
                    if i == chunkSize - 1 {
                        entryId = entry.([]interface {})[0].(string)
                        logger.Debug().
                            Str("nextId", entryId).
                            Str("stream", stream).
                            Int("chunkSize", chunkSize).
                            Msg("Read a chunk of entries from Redis stream")
                    }
                    writer.WriteString(fmt.Sprintf(`{"value": %q}`, entry))
                }
                writer.Flush()
            } else {
                writer.WriteString(fmt.Sprintf(`{"value": %q}`, err))
                writer.Flush()
                break;
            }
/*            result, err := redisConn.Do("XREAD", "COUNT", *xreadCount, "BLOCK", 0, *stream, "$")
            if err != nil {
                errId := uuid.Must(uuid.NewV4()).String()
                logger.Error().
                    Err(err).
                    Str("stream", *stream).
                    Uint64("connId", connId).
                    Uint16("code", 2).
                    Str("errId", errId).
                    Msg("Failed to read a stream from Redis")
                writer.WriteString(`{"type":"error","id":"`)
                writer.WriteString(errId)
                writer.WriteString(`","code":2,"msg":"Failed to read a stream from Redis","stream":"`)
                writer.WriteString(*stream)
                writer.WriteString(`"}`)
                writer.Flush()
                break;
            } else {
                for _, msg := range result[0].Messages {
                    writer.WriteString(`{"type":"event","id":"`)
                    writer.WriteString(msg.ID)
                    writer.WriteString(`","payload":`)
                    writer.WriteString(msg.Values["payload"].(string))
                    writer.WriteString("}")
                }
                writer.Flush()
            }
*/        }
    }
}

func parseAnchor(anchor string) Anchor {
    stream := anchor[:strings.Index(anchor, ";")]
    trimmed := anchor[len(stream) + 1:]
    firstId := trimmed[:strings.Index(trimmed, ";")]
    lastId := trimmed[len(firstId) + 1:]
    return Anchor{Stream: stream, FirstId: firstId, LastId: lastId}
}

func (redis Redis) loadAnchors(story string) ([]Anchor, error) {
    result, err := redis.Conn.Do("LRANGE", redis.KeyPrefix + "story:" + story, 0, -1)
    if err == nil {
        anchors := make([]Anchor, len(result.([]string)))
        for i, a := range result.([]string) {
            anchors[i] = parseAnchor(a)
        }
        return anchors, nil
    } else {
        return nil, err
    }
}

func toEntries(result interface {}) []interface {} {
    return result.([]interface {})[0].([]interface {})[1].([]interface {})
}

func isLastBatch(batch []interface {}, lastId string) bool {
    for _, entry := range batch {
        if entry.([]interface {})[0] == lastId {
            return true
        }
    }
    return false
}

func lastIdInBatch(batch []interface {}) string {
    return batch[len(batch) - 1].([]interface {})[0].(string)
}

func toWriter(writer *bufio.Writer, stream string, id string, payload string) {
    writer.WriteString(`{"type":"event","id":"`)
    writer.WriteString(stream)
    writer.WriteString("-")
    writer.WriteString(id)
    writer.WriteString(`","payload":`)
    writer.WriteString(payload)
    writer.WriteString("}")
}

func (redis Redis) pump(anchors []Anchor, writer *bufio.Writer) {
    for _, anchor := range anchors {
        firstId := anchor.FirstId
        for {
            xReadArgs := []interface {}{
                "COUNT", redis.BatchSize, "BLOCK", 0, "STREAMS", anchor.Stream, firstId,
            }
            batch, err := redis.Conn.Do("XREAD", xReadArgs...)
            if batch == nil {
                // this guard is useful in test with incorrectly mocked redis
                // TODO logging and writer output
            } else if err == nil {
                entries := toEntries(batch)
                for _, entry := range entries {
                    e := entry.([]interface {})
                    toWriter(
                        writer,
                        anchor.Stream,
                        e[0].(string),
                        e[1].([]interface {})[1].(string),
                    )
                    writer.Flush() // TODO
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
