package service

import (
	"path/filepath"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestParseTabularRecordsKeepsMaxColumns(t *testing.T) {
	parsed, err := parseTabularRecords([][]string{
		{"name", "phone"},
		{"alice", "123", "sales"},
		{"bob", "456", "support"},
	})
	if err != nil {
		t.Fatalf("parseTabularRecords returned error: %v", err)
	}

	if len(parsed.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(parsed.Columns))
	}
	if parsed.Columns[2].Code != "column_3" {
		t.Fatalf("expected fallback third column name, got %q", parsed.Columns[2].Code)
	}
	if got := parsed.Rows[0]["column_3"]; got != "sales" {
		t.Fatalf("expected third column value to be preserved, got %q", got)
	}
}

func TestParseExcelFileCombinesSheets(t *testing.T) {
	tmpDir := t.TempDir()
	xlsxPath := filepath.Join(tmpDir, "enterprise.xlsx")

	f := excelize.NewFile()
	f.SetSheetName("Sheet1", "Customers")
	_ = f.SetSheetRow("Customers", "A1", &[]interface{}{"name", "phone"})
	_ = f.SetSheetRow("Customers", "A2", &[]interface{}{"alice", "123"})

	orders := "Orders"
	f.NewSheet(orders)
	_ = f.SetSheetRow(orders, "A1", &[]interface{}{"order_no", "amount", "status"})
	_ = f.SetSheetRow(orders, "A2", &[]interface{}{"SO-1", "88", "paid"})

	if err := f.SaveAs(xlsxPath); err != nil {
		t.Fatalf("SaveAs returned error: %v", err)
	}

	parsed, err := parseExcelFile(xlsxPath)
	if err != nil {
		t.Fatalf("parseExcelFile returned error: %v", err)
	}

	if len(parsed) != 2 {
		t.Fatalf("expected 2 parsed sheets, got %d", len(parsed))
	}
	if parsed[0].SheetName != "Customers" || parsed[1].SheetName != "Orders" {
		t.Fatalf("expected parsed sheet names to be preserved, got %q and %q", parsed[0].SheetName, parsed[1].SheetName)
	}
	if len(parsed[0].Columns) != 2 || parsed[0].Columns[0].Code != "name" || parsed[0].Columns[1].Code != "phone" {
		t.Fatalf("expected first sheet columns to be name/phone, got %#v", parsed[0].Columns)
	}
	if len(parsed[1].Columns) != 3 || parsed[1].Columns[0].Code != "order_no" || parsed[1].Columns[2].Code != "status" {
		t.Fatalf("expected second sheet columns to be preserved, got %#v", parsed[1].Columns)
	}
	if parsed[0].Rows[0]["name"] != "alice" || parsed[0].Rows[0]["phone"] != "123" {
		t.Fatalf("expected first sheet row data to be preserved, got %#v", parsed[0].Rows[0])
	}
	if parsed[1].Rows[0]["order_no"] != "SO-1" || parsed[1].Rows[0]["amount"] != "88" || parsed[1].Rows[0]["status"] != "paid" {
		t.Fatalf("expected second sheet row data to be preserved, got %#v", parsed[1].Rows[0])
	}
}
