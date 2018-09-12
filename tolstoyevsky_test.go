package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"github.com/google/go-cmp/cmp"
	"github.com/rafaeljusto/redigomock"
	"github.com/satori/go.uuid"
	"github.com/valyala/fasthttp"
	"strings"
	"testing"
)

type ParsedError struct {
	Type  string `json:"type"`
	Msg   string `json:"msg"`
	Cause string `json:"cause"`
	Id    string `json:"id"`
	Key1  string `json:"key1"`
	Key2  string `json:"key2"`
}

type HttpContextMock struct {
	ConnId uint64
	Buffer *bytes.Buffer
}

func (HttpContextMock) SetBodyStreamWriter(sw fasthttp.StreamWriter) {
	panic("func (HttpContextMock) SetBodyStreamWriter(sw fasthttp.StreamWriter) should not be called")
}

func (m HttpContextMock) ConnID() uint64 {
	return m.ConnId
}

func (HttpContextMock) Error(msg string, statusCode int) {
	parsed := &ParsedError{}

	json.NewDecoder(strings.NewReader(msg)).Decode(parsed)

	// then
	if parsed.Msg != "failed to XREAD" {
		panic("unexpected msg: " + parsed.Msg)
	}
}

func (HttpContextMock) ResetBody() {
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

func TestWriteInfo(t *testing.T) {
	t.Parallel()

	// given
	buf := new(bytes.Buffer)
	ctx := StoryReadCtx{
		Story:      "myStory",
		HttpCtx:    &HttpContextMock{ConnId: 42},
		HttpWriter: bufio.NewWriter(buf),
	}
	expected := `{"type":"info","msg":"Some description","key1":"value1","key2":"value2"}`

	// when
	ctx.writeInfo("Some description", "key1", "value1", "key2", "value2")
	ctx.HttpWriter.Flush()

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}

}

func TestWriteError(t *testing.T) {
	t.Parallel()

	// given
	buf := new(bytes.Buffer)
	ctx := StoryReadCtx{
		Story:      "myStory",
		HttpCtx:    &HttpContextMock{ConnId: 42},
		HttpWriter: bufio.NewWriter(buf),
	}
	parsed := &ParsedError{}

	// when
	ctx.writeError(
		errors.New("some error"),
		"Some description",
		"key1", "value1", "key2", "value2",
	)
	ctx.HttpWriter.Flush()
	json.NewDecoder(buf).Decode(parsed)

	// then
	if parsed.Type != "error" {
		t.Errorf("type: %s != error", parsed.Type)
	}
	if parsed.Msg != "Some description" {
		t.Errorf("msg: %s != Some description", parsed.Msg)
	}
	if parsed.Cause != "some error" {
		t.Errorf("cause: %s != some error", parsed.Cause)
	}
	if _, err := uuid.FromString(parsed.Id); err != nil {
		t.Errorf("id: %s is not valid UUID", parsed.Id)
	}
	if parsed.Key1 != "value1" {
		t.Errorf("key1: %s != value1", parsed.Key1)
	}
	if parsed.Key2 != "value2" {
		t.Errorf("key2: %s != value2", parsed.Key2)
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
		Story:          "story1",
	}
	buf := new(bytes.Buffer)
	expected :=
		`{"type":"event","id":"p:stories:story1-12345-1","payload":{"value": 123}}` +
			`{"type":"event","id":"p:stories:story1-67890-0","payload":{"value": 124}}` +
			`{"type":"event","id":"p:stories:story1-67890-1","payload":{"value": 125}}` +
			`{"type":"event","id":"p:stories:story1-78901-0","payload":{"value": 126}}` +
			`{"type":"event","id":"p:stories:story1-89012-0","payload":{"value": 127}}` +
			`{"type":"event","id":"p:stories:story1-89012-1","payload":{"value": 128}}` +
			`{"type":"event","id":"p:stories:story1-89012-2","payload":{"value": 129}}`
	// the last entry is not flushed

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
	ctx.pump(nil, bufio.NewWriter(buf))

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}
}

func TestPumpError(t *testing.T) {
	t.Parallel()

	// given
	var contexts CtxRegistryImpl
	conn := redigomock.NewConn()
	buf := new(bytes.Buffer)
	ctx := StoryReadCtx{
		RedisConn:      conn,
		KeyPrefix:      "p:",
		XReadCount:     2,
		EntriesToFlush: 7,
		HttpCtx:        &HttpContextMock{ConnId: 42, Buffer: buf},
		Story:          "story1",
		Registry:       &contexts,
	}
	expected :=
		`{"type":"event","id":"p:stories:story1-12345-1","payload":{"value": 123}}` +
			`{"type":"event","id":"p:stories:story1-67890-0","payload":{"value": 124}}`

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
	ctx.pump(nil, bufio.NewWriter(buf))

	// then
	if buf.String() != expected {
		t.Errorf("buf.toString(): %s != %s", buf.String(), expected)
	}
}
