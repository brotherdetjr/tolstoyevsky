package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/rafaeljusto/redigomock"
	"github.com/valyala/fasthttp"
	"testing"
)

type HttpContextMock struct {
	ConnId uint64
}

func (HttpContextMock) SetBodyStreamWriter(sw fasthttp.StreamWriter) {
	panic("should not be called")
}

func (m HttpContextMock) ConnID() uint64 {
	return m.ConnId
}

func (HttpContextMock) Error(msg string, statusCode int) {
	panic("should not be called")
}

func TestParseAnchor(t *testing.T) {
	t.Parallel()

	// when
	anchor := parseAnchor([]byte("prefix:mystream;12345-1;67890-2"))

	// then
	if !bytes.Equal(anchor.Stream, []byte("prefix:mystream")) {
		t.Errorf(`Stream: %q != "prefix:mystream"`, anchor.Stream)
	}
	if anchor.FirstId != "12345-1" {
		t.Errorf(`FirstId: %q != "12345-1"`, anchor.FirstId)
	}
	if !bytes.Equal(anchor.LastId, []byte("67890-2")) {
		t.Errorf(`LastId: %q != "67890-2"`, anchor.LastId)
	}
}

func TestParseAnchorNoLastId(t *testing.T) {
	t.Parallel()

	// when
	anchor := parseAnchor([]byte("prefix:mystream;12345-1;"))

	// then
	if !bytes.Equal(anchor.Stream, []byte("prefix:mystream")) {
		t.Errorf(`Stream: %q != "prefix:mystream"`, anchor.Stream)
	}
	if anchor.FirstId != "12345-1" {
		t.Errorf(`FirstId: %q != "12345-1"`, anchor.FirstId)
	}
	if len(anchor.LastId) != 0 {
		t.Errorf(`LastId: %q != ""`, anchor.LastId)
	}
}

func TestParseCornerCase(t *testing.T) {
	t.Parallel()

	// when
	anchor := parseAnchor([]byte("a;b;c"))

	// then
	if !bytes.Equal(anchor.Stream, []byte("a")) {
		t.Errorf(`Stream: %q != "a"`, anchor.Stream)
	}
	if anchor.FirstId != "b" {
		t.Errorf(`FirstId: %q != "b"`, anchor.FirstId)
	}
	if !bytes.Equal(anchor.LastId, []byte("c")) {
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
	).Expect([]interface{}{
		[]byte("prefix:mystream;12345-1;67890-2"),
		[]byte("prefix:mystream;12345-1;"),
		[]byte("a;b;c"),
	})
	expected := []Anchor{
		{Stream: []byte("prefix:mystream"), FirstId: "12345-1", LastId: []byte("67890-2")},
		{Stream: []byte("prefix:mystream"), FirstId: "12345-1", LastId: []byte("")},
		{Stream: []byte("a"), FirstId: "b", LastId: []byte("c")},
	}
	ctx := StoryReadCtx{RedisConn: conn, KeyPrefix: "tolstoyevsky:"}

	// when
	anchors, err := ctx.loadAnchors("mystory")

	// then
	if !cmp.Equal(anchors, expected) {
		t.Errorf("anchors: %s != %s", anchors, expected)
	}
	if err != nil {
		t.Errorf("err: %s != nil", err)
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
	).ExpectError(fmt.Errorf("simulate error"))
	ctx := StoryReadCtx{RedisConn: conn, KeyPrefix: "tolstoyevsky:"}

	// when
	anchors, err := ctx.loadAnchors("mystory")

	// then
	if anchors != nil {
		t.Errorf("anchors: %s != nil", anchors)
	}
	if err == nil {
		t.Errorf("err == nil")
	}
}

func TestToEntries(t *testing.T) {
	t.Parallel()

	// given
	entries := []interface{}{
		[]interface{}{
			"12345-1",
			[]interface{}{
				"payload",
				[]byte(`{"value": 123}`),
			},
		},
		[]interface{}{
			"67890-0",
			[]interface{}{
				"payload",
				[]byte(`{"value": 124}`),
			},
		},
	}
	batch := []interface{}{[]interface{}{"p:stream1", entries}}

	// expect
	if !cmp.Equal(toEntries(batch), entries) {
		t.Fail()
	}
}

func TestIsLastBatch(t *testing.T) {
	t.Parallel()

	// given
	batch := []interface{}{
		[]interface{}{
			"12345-1",
			[]interface{}{
				"payload",
				[]byte(`{"value": 123}`),
			},
		},
		[]interface{}{
			"67890-0",
			[]interface{}{
				"payload",
				[]byte(`{"value": 124}`),
			},
		},
	}

	// expect
	if !isLastBatch(batch, []byte("12345-1")) {
		t.Error("LastId = 12345-1: should be the last batch!")
	}
	if !isLastBatch(batch, []byte("67890-0")) {
		t.Error("LastId = 67890-0: should be the last batch!")
	}
	if isLastBatch(batch, []byte("123")) {
		t.Error("LastId = 123: should not be the last batch!")
	}
	if isLastBatch(batch, nil) {
		t.Error("LastId = \"\": should not be the last batch!")
	}

}

func TestLastIdInBatch(t *testing.T) {
	t.Parallel()

	// given
	batch := []interface{}{
		[]interface{}{
			"12345-1",
			[]interface{}{
				"payload",
				[]byte(`{"value": 123}`),
			},
		},
		[]interface{}{
			"67890-0",
			[]interface{}{
				"payload",
				[]byte(`{"value": 124}`),
			},
		},
	}

	// expect
	if lastIdInBatch(batch) != "67890-0" {
		t.Fail()
	}
}

func TestLastIdInBatchSingleEntry(t *testing.T) {
	t.Parallel()

	// given
	batch := []interface{}{
		[]interface{}{
			"67890-0",
			[]interface{}{
				"payload",
				[]byte(`{"value": 124}`),
			},
		},
	}

	// expect
	if lastIdInBatch(batch) != "67890-0" {
		t.Fail()
	}
}

func TestPump(t *testing.T) {
	t.Parallel()

	// given
	conn := redigomock.NewConn()
	ctx := StoryReadCtx{
		RedisConn:      conn,
		KeyPrefix:      "p:",
		XReadCount:     2,
		EntriesToFlush: 7,
		HttpCtx:        &HttpContextMock{ConnId: 42},
	}
	anchors := []Anchor{
		{Stream: []byte("p:stream1"), FirstId: "0", LastId: []byte("67890-1")},
		{Stream: []byte("p:stream2"), FirstId: "78900-0", LastId: []byte("78901-0")},
		{Stream: []byte("p:stream3"), FirstId: "0", LastId: []byte("89012-3")},
	}
	buf := new(bytes.Buffer)
	expected :=
		`{"type":"event","id":"p:stream1-12345-1","payload":{"value": 123}}` +
			`{"type":"event","id":"p:stream1-67890-0","payload":{"value": 124}}` +
			`{"type":"event","id":"p:stream1-67890-1","payload":{"value": 125}}` +
			`{"type":"event","id":"p:stream2-78901-0","payload":{"value": 126}}` +
			`{"type":"event","id":"p:stream3-89012-0","payload":{"value": 127}}` +
			`{"type":"event","id":"p:stream3-89012-1","payload":{"value": 128}}` +
			`{"type":"event","id":"p:stream3-89012-2","payload":{"value": 129}}`
	// the last entry is not flushed

	// interactions
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", []byte("p:stream1"), "0").
		Expect([]interface{}{
			[]interface{}{
				"p:stream1",
				[]interface{}{
					[]interface{}{
						"12345-1",
						[]interface{}{
							"payload",
							[]byte(`{"value": 123}`),
						},
					},
					[]interface{}{
						"67890-0",
						[]interface{}{
							"payload",
							[]byte(`{"value": 124}`),
						},
					},
				},
			},
		})
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", []byte("p:stream1"), "67890-0").
		Expect([]interface{}{
			[]interface{}{
				"p:stream1",
				[]interface{}{
					[]interface{}{
						"67890-1",
						[]interface{}{
							"payload",
							[]byte(`{"value": 125}`),
						},
					},
				},
			},
		})
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", []byte("p:stream2"), "78900-0").
		Expect([]interface{}{
			[]interface{}{
				"p:stream2",
				[]interface{}{
					[]interface{}{
						"78901-0",
						[]interface{}{
							"payload",
							[]byte(`{"value": 126}`),
						},
					},
				},
			},
		})
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", []byte("p:stream3"), "0").
		Expect([]interface{}{
			[]interface{}{
				"p:stream3",
				[]interface{}{
					[]interface{}{
						"89012-0",
						[]interface{}{
							"payload",
							[]byte(`{"value": 127}`),
						},
					},
					[]interface{}{
						"89012-1",
						[]interface{}{
							"payload",
							[]byte(`{"value": 128}`),
						},
					},
				},
			},
		})
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", []byte("p:stream3"), "89012-1").
		Expect([]interface{}{
			[]interface{}{
				"p:stream3",
				[]interface{}{
					[]interface{}{
						"89012-2",
						[]interface{}{
							"payload",
							[]byte(`{"value": 129}`),
						},
					},
					[]interface{}{
						"89012-3",
						[]interface{}{
							"payload",
							[]byte(`{"value": 130}`),
						},
					},
				},
			},
		})

	// when
	ctx.pump(anchors, bufio.NewWriter(buf))

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}
}
