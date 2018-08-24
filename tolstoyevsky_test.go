package main

import (
    "testing"
)

func TestParseAnchor(t *testing.T) {
    t.Parallel()
    stream, firstId, lastId := parseAnchor("prefix:mystream;12345-1;67890-2")
    if stream != "prefix:mystream" {
        t.Errorf(`%q != "prefix:mystream"`, stream)
    }
    if firstId != "12345-1" {
        t.Errorf(`%q != "12345-1"`, firstId)
    }
    if lastId != "67890-2" {
        t.Errorf(`%q != "67890-2"`, lastId)
    }
}

func TestParseAnchorNoLastId(t *testing.T) {
    t.Parallel()
    stream, firstId, lastId := parseAnchor("prefix:mystream;12345-1;")
    if stream != "prefix:mystream" {
        t.Errorf(`%q != "prefix:mystream"`, stream)
    }
    if firstId != "12345-1" {
        t.Errorf(`%q != "12345-1"`, firstId)
    }
    if lastId != "" {
        t.Errorf(`%q != ""`, lastId)
    }
}

func TestParseCornerCase(t *testing.T) {
    t.Parallel()
    stream, firstId, lastId := parseAnchor("a;b;c")
    if stream != "a" {
        t.Errorf(`%q != "a"`, stream)
    }
    if firstId != "b" {
        t.Errorf(`%q != "b"`, firstId)
    }
    if lastId != "c" {
        t.Errorf(`%q != "c"`, lastId)
    }
}
