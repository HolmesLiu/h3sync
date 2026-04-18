package service

import (
	"strings"
	"testing"
	"time"
)

func TestBuildWhereNumericObjectIDComparison(t *testing.T) {
	where, args, err := buildWhere([]QueryFilter{
		{Field: "object_id", Operator: "gt", Value: "99999"},
	})
	if err != nil {
		t.Fatalf("buildWhere returned error: %v", err)
	}
	expected := "(object_id ~ '^[0-9]+$' AND object_id::numeric > $1::numeric)"
	if where != expected {
		t.Fatalf("unexpected where clause:\nwant: %s\ngot:  %s", expected, where)
	}
	if len(args) != 1 || args[0] != "99999" {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestBuildWhereRejectsUnknownOperator(t *testing.T) {
	_, _, err := buildWhere([]QueryFilter{
		{Field: "object_id", Operator: "between", Value: "1"},
	})
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
}

func TestEncodeDecodeQueryCursor(t *testing.T) {
	now := time.Date(2026, 4, 18, 10, 30, 0, 0, time.UTC)
	raw, err := encodeQueryCursorFromRow(map[string]interface{}{
		"object_id":     "100001",
		"modified_time": now,
	})
	if err != nil {
		t.Fatalf("encodeQueryCursorFromRow returned error: %v", err)
	}
	cursor, err := decodeQueryCursor(raw)
	if err != nil {
		t.Fatalf("decodeQueryCursor returned error: %v", err)
	}
	if cursor.ObjectID != "100001" {
		t.Fatalf("unexpected object id: %s", cursor.ObjectID)
	}
	if cursor.ModifiedTime == nil || !cursor.ModifiedTime.Equal(now) {
		t.Fatalf("unexpected modified time: %#v", cursor.ModifiedTime)
	}
}

func TestDecodeQueryCursorRejectsInvalidValue(t *testing.T) {
	_, err := decodeQueryCursor("bad-cursor")
	if err == nil {
		t.Fatal("expected invalid cursor error")
	}
}

func TestEncodeQueryCursorAllowsNilModifiedTime(t *testing.T) {
	raw, err := encodeQueryCursorFromRow(map[string]interface{}{
		"object_id": "manual_row_1",
	})
	if err != nil {
		t.Fatalf("encodeQueryCursorFromRow returned error: %v", err)
	}
	if strings.TrimSpace(raw) == "" {
		t.Fatal("expected non-empty cursor")
	}
}
