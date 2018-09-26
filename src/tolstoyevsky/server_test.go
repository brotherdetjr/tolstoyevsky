package tolstoyevsky

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/rafaeljusto/redigomock"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
	"sync"
	"testing"
)

var logger = zerolog.Nop()

type httpContextMock struct {
	connId uint64
	buffer *bytes.Buffer
}

func (httpContextMock) SetBodyStreamWriter(sw fasthttp.StreamWriter) {
	panic("func (HttpContextMock) SetBodyStreamWriter(sw fasthttp.StreamWriter) should not be called")
}

func (m httpContextMock) ConnID() uint64 {
	return m.connId
}

func (httpContextMock) Error(msg string, statusCode int) {
	expected := `id: 999
event: error
data: {"description":"Failed to XREAD","cause":"ignored","stream":"p:stories:story1"}

`

	// then
	if msg != expected {
		panic("unexpected msg: " + msg)
	}

	if statusCode != 500 {
		panic(fmt.Sprintf("unexpected statusCode: %d", statusCode))
	}
}

func (httpContextMock) ResetBody() {
	// nothing
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

func TestWriteShutdown(t *testing.T) {
	t.Parallel()

	// given
	buf := new(bytes.Buffer)
	ctx := storyReadCtx{
		story:        "myStory",
		httpCtx:      &httpContextMock{connId: 42},
		httpWriter:   bufio.NewWriter(buf),
		logger:       &logger,
		uuidSupplier: func() string { return "999" },
	}
	expected := `id: 999
event: shutdown
data: 0

`

	// when
	ctx.writeShutdown()
	ctx.httpWriter.Flush()

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}

}

func TestWriteError(t *testing.T) {
	t.Parallel()

	// given
	buf := new(bytes.Buffer)
	ctx := storyReadCtx{
		story:        "myStory",
		httpCtx:      &httpContextMock{connId: 42},
		httpWriter:   bufio.NewWriter(buf),
		logger:       &logger,
		uuidSupplier: func() string { return "999" },
	}
	expected := `id: 999
event: error
data: {"description":"Some description","cause":"some error","key1":"value1","key2":"value2"}

`

	// when
	ctx.writeGetError(
		errors.New("some error"),
		"Some description",
		"key1", "value1", "key2", "value2",
	)
	ctx.httpWriter.Flush()

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}
}

func TestPump(t *testing.T) {
	t.Parallel()

	// given
	conn := redigomock.NewConn()
	ctx := storyReadCtx{
		redisConn:      conn,
		keyPrefix:      "p:",
		xReadCount:     2,
		entriesToFlush: 7,
		httpCtx:        &httpContextMock{connId: 42},
		story:          "story1",
		logger:         &logger,
	}
	buf := new(bytes.Buffer)
	expected := `id: p:stories:story1-12345-1
event: entry
data: {"value": 123}

id: p:stories:story1-67890-0
event: entry
data: {"value": 124}

id: p:stories:story1-67890-1
event: entry
data: {"value": 125}

id: p:stories:story1-78901-0
event: entry
data: {"value": 126}

id: p:stories:story1-89012-0
event: entry
data: {"value": 127}

id: p:stories:story1-89012-1
event: entry
data: {"value": 128}

id: p:stories:story1-89012-2
event: entry
data: {"value": 129}

id: p:stories:story1-89012-3
event: entry
data: {"value": 130}

`

	// interactions
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "0").
		Expect([]interface{}{
			[]interface{}{
				"p:stories:story1",
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
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "67890-0").
		Expect([]interface{}{
			[]interface{}{
				"p:stories:story1",
				[]interface{}{
					[]interface{}{
						"67890-1",
						[]interface{}{
							"payload",
							[]byte(`{"value": 125}`),
						},
					},
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
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "78901-0").
		Expect([]interface{}{
			[]interface{}{
				"p:stories:story1",
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
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "89012-1").
		Expect([]interface{}{
			[]interface{}{
				"p:stories:story1",
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

	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "89012-3").
		Expect(nil)

	// when
	ctx.pump(bufio.NewWriter(buf))

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}
}

func TestPumpError(t *testing.T) {
	t.Parallel()

	// given
	conn := redigomock.NewConn()
	buf := new(bytes.Buffer)
	var contexts sync.Map
	ctx := storyReadCtx{
		redisConn:      conn,
		keyPrefix:      "p:",
		xReadCount:     2,
		entriesToFlush: 7,
		httpCtx:        &httpContextMock{connId: 42, buffer: buf},
		story:          "story1",
		logger:         &logger,
		contexts:       &contexts,
		uuidSupplier:   func() string { return "999" },
	}
	expected := `id: p:stories:story1-12345-1
event: entry
data: {"value": 123}

id: p:stories:story1-67890-0
event: entry
data: {"value": 124}

`
	// interactions
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "0").
		Expect([]interface{}{
			[]interface{}{
				"p:stories:story1",
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
	conn.Command("XREAD", "COUNT", uint(2), "BLOCK", 0, "STREAMS", "p:stories:story1", "67890-0").
		ExpectError(errors.New("ignored"))

	// when
	ctx.pump(bufio.NewWriter(buf))

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}
}
