package main

import (
    "fmt"
    "testing"
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
