package main

import (
    "testing"
)

func TestParseAnchor(t *testing.T) {
    t.Parallel()
    anchor := parseAnchor("prefix:mystream;12345-1;67890-2")
    if anchor.Stream != "prefix:mystream" {
        t.Errorf(`%q != "prefix:mystream"`, anchor.Stream)
    }
    if anchor.FirstId != "12345-1" {
        t.Errorf(`%q != "12345-1"`, anchor.FirstId)
    }
    if anchor.LastId != "67890-2" {
        t.Errorf(`%q != "67890-2"`, anchor.LastId)
    }
}

func TestParseAnchorNoLastId(t *testing.T) {
    t.Parallel()
    anchor := parseAnchor("prefix:mystream;12345-1;")
    if anchor.Stream != "prefix:mystream" {
        t.Errorf(`%q != "prefix:mystream"`, anchor.Stream)
    }
    if anchor.FirstId != "12345-1" {
        t.Errorf(`%q != "12345-1"`, anchor.FirstId)
    }
    if anchor.LastId != "" {
        t.Errorf(`%q != ""`, anchor.LastId)
    }
}

func TestParseCornerCase(t *testing.T) {
    t.Parallel()
    anchor := parseAnchor("a;b;c")
    if anchor.Stream != "a" {
        t.Errorf(`%q != "a"`, anchor.Stream)
    }
    if anchor.FirstId != "b" {
        t.Errorf(`%q != "b"`, anchor.FirstId)
    }
    if anchor.LastId != "c" {
        t.Errorf(`%q != "c"`, anchor.LastId)
    }
}
