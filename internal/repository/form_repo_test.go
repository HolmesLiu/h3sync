package repository

import (
	"strings"
	"testing"
	"time"
)

func TestBuildQueryCursorConditionWithModifiedTime(t *testing.T) {
	now := time.Date(2026, 4, 18, 9, 0, 0, 0, time.UTC)
	clause, args := buildQueryCursorCondition(3, &now, "100000")
	if !strings.Contains(clause, "modified_time < $3") {
		t.Fatalf("unexpected clause: %s", clause)
	}
	if !strings.Contains(clause, "object_id::numeric < $4::numeric") {
		t.Fatalf("unexpected clause: %s", clause)
	}
	if len(args) != 2 {
		t.Fatalf("unexpected args len: %d", len(args))
	}
}

func TestBuildQueryCursorConditionWithoutModifiedTime(t *testing.T) {
	clause, args := buildQueryCursorCondition(5, nil, "manual_9")
	if !strings.Contains(clause, "modified_time IS NULL") {
		t.Fatalf("unexpected clause: %s", clause)
	}
	if !strings.Contains(clause, "object_id < $6") {
		t.Fatalf("unexpected clause: %s", clause)
	}
	if len(args) != 1 || args[0] != "manual_9" {
		t.Fatalf("unexpected args: %#v", args)
	}
}
