package main

import (
    "fmt"
    "testing"
    "bytes"
    "bufio"

    "github.com/rafaeljusto/redigomock"
    "github.com/google/go-cmp/cmp"
)

func TestParseAnchor(t *testing.T) {
    t.Parallel()

    // when
    anchor := parseAnchor("prefix:mystream;12345-1;67890-2")

    // then
    if anchor.Stream != "prefix:mystream" {
        t.Errorf(`Stream: %q != "prefix:mystream"`, anchor.Stream)
    }
    if anchor.FirstId != "12345-1" {
        t.Errorf(`FirstId: %q != "12345-1"`, anchor.FirstId)
    }
    if anchor.LastId != "67890-2" {
        t.Errorf(`LastId: %q != "67890-2"`, anchor.LastId)
    }
}

func TestParseAnchorNoLastId(t *testing.T) {
    t.Parallel()

    // when
    anchor := parseAnchor("prefix:mystream;12345-1;")

    // then
    if anchor.Stream != "prefix:mystream" {
        t.Errorf(`Stream: %q != "prefix:mystream"`, anchor.Stream)
    }
    if anchor.FirstId != "12345-1" {
        t.Errorf(`FirstId: %q != "12345-1"`, anchor.FirstId)
    }
    if anchor.LastId != "" {
        t.Errorf(`LastId: %q != ""`, anchor.LastId)
    }
}

func TestParseCornerCase(t *testing.T) {
    t.Parallel()

    // when
    anchor := parseAnchor("a;b;c")

    // then
    if anchor.Stream != "a" {
        t.Errorf(`Stream: %q != "a"`, anchor.Stream)
    }
    if anchor.FirstId != "b" {
        t.Errorf(`FirstId: %q != "b"`, anchor.FirstId)
    }
    if anchor.LastId != "c" {
        t.Errorf(`LastId: %q != "c"`, anchor.LastId)
    }
}

func TestLoadAnchors(t *testing.T) {
    t.Parallel()

    // given
    conn := redigomock.NewConn()
    conn.Command(
        "LRANGE",
        "tolstoyevsky:story:mystory",
        0,
        -1,
    ).Expect([]string{
        "prefix:mystream;12345-1;67890-2",
        "prefix:mystream;12345-1;",
        "a;b;c",
    })
    expected := []Anchor{
        Anchor{Stream: "prefix:mystream", FirstId: "12345-1", LastId: "67890-2"},
        Anchor{Stream: "prefix:mystream", FirstId: "12345-1", LastId: ""},
        Anchor{Stream: "a", FirstId: "b", LastId: "c"},
    }
    redis := Redis{Conn: conn, KeyPrefix: "tolstoyevsky:"}

    // when
    anchors, err := redis.loadAnchors("mystory")

    // then
    if !cmp.Equal(anchors, expected) {
        t.Errorf("anchors: %v != %v", anchors, expected)
    }
    if err != nil {
        t.Errorf("err: %v != nil", err)
    }
}

func TestLoadAnchorsError(t *testing.T) {
    t.Parallel()

    // given
    conn := redigomock.NewConn()
    conn.Command(
        "LRANGE",
        "tolstoyevsky:story:mystory",
        0,
        -1,
    ).ExpectError(fmt.Errorf("Simulate error!"))
    redis := Redis{Conn: conn, KeyPrefix: "tolstoyevsky:"}

    // when
    anchors, err := redis.loadAnchors("mystory")

    // then
    if anchors != nil {
        t.Errorf("anchors: %v != nil", anchors)
    }
    if err == nil {
        t.Errorf("err == nil")
    }
}

func TestReadStory(t *testing.T) {
    t.Parallel()

    // given
    conn := redigomock.NewConn()
    redis := Redis{Conn: conn, KeyPrefix: "p:", BatchSize: 2}
    anchors := []Anchor{
        Anchor{Stream: "p:stream1", FirstId: "0", LastId: "67890-2"},
        Anchor{Stream: "p:stream2", FirstId: "78900-0", LastId: "78901-0"},
        Anchor{Stream: "p:stream3", FirstId: "0", LastId: ""},
    }
    buf := new(bytes.Buffer)
    expected :=
        `{"type":"event","id":"p:stream1-12345-1","payload":{"value": 123}}` +
        `{"type":"event","id":"p:stream1-67890-0","payload":{"value": 124}}` +
        `{"type":"event","id":"p:stream1-67890-1","payload":{"value": 125}}` +
        `{"type":"event","id":"p:stream2-78901-0","payload":{"value": 126}}` +
        `{"type":"event","id":"p:stream3-89012-0","payload":{"value": 127}}` +
        `{"type":"event","id":"p:stream3-89012-1","payload":{"value": 128}}` +
        `{"type":"event","id":"p:stream3-89012-2","payload":{"value": 129}}` +
        `{"type":"event","id":"p:stream3-89012-3","payload":{"value": 130}}`

    // interactions
    conn.Command("XREAD", "BLOCK", 0, "COUNT", 2, "STREAMS", "p:stream1", "0").
        Expect([]interface {}{
            []interface {}{
                "p:stream1",
                []interface {}{
                    "12345-1",
                    []interface {}{
                        "payload",
                        `{"value": 123}`,
                    },
                },
                []interface {}{
                    "67890-0",
                    []interface {}{
                        "payload",
                        `{"value": 124}`,
                    },
                },
            },
        })
    conn.Command("XREAD", "BLOCK", 0, "COUNT", 2, "STREAMS", "p:stream1", "67890-0").
        Expect([]interface {}{
            []interface {}{
                "p:stream1",
                []interface {}{
                    "67890-1",
                    []interface {}{
                        "payload",
                        `{"value": 125}`,
                    },
                },
            },
        })
    conn.Command("XREAD", "BLOCK", 0, "COUNT", 2, "STREAMS", "p:stream2", "78900-0").
        Expect([]interface {}{
            []interface {}{
                "p:stream2",
                []interface {}{
                    "78901-0",
                    []interface {}{
                        "payload",
                        `{"value": 126}`,
                    },
                },
            },
        })
    conn.Command("XREAD", "BLOCK", 0, "COUNT", 2, "STREAMS", "p:stream3", "0").
        Expect([]interface {}{
            []interface {}{
                "p:stream3",
                []interface {}{
                    "89012-0",
                    []interface {}{
                        "payload",
                        `{"value": 127}`,
                    },
                },
                []interface {}{
                    "89012-1",
                    []interface {}{
                        "payload",
                        `{"value": 128}`,
                    },
                },
            },
        })
    conn.Command("XREAD", "BLOCK", 0, "COUNT", 2, "STREAMS", "p:stream3", "89012-1").
        Expect([]interface {}{
            []interface {}{
                "p:stream3",
                []interface {}{
                    "89012-2",
                    []interface {}{
                        "payload",
                        `{"value": 129}`,
                    },
                },
                []interface {}{
                    "89012-3",
                    []interface {}{
                        "payload",
                        `{"value": 130}`,
                    },
                },
            },
        })

    // when
    redis.pump(anchors, bufio.NewWriter(buf))

    // then
    if buf.String() != expected {
        t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
    }
}
