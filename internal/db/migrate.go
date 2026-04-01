package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
)

func Migrate(db *sqlx.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		);
	`); err != nil {
		return err
	}

	entries, err := os.ReadDir("migrations")
	if err != nil {
		return err
	}

	versions := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		versions = append(versions, e.Name())
	}
	sort.Strings(versions)

	for _, v := range versions {
		var exists int
		if err := db.Get(&exists, "SELECT 1 FROM schema_migrations WHERE version=$1", v); err == nil && exists == 1 {
			continue
		}

		b, err := os.ReadFile(filepath.Join("migrations", v))
		if err != nil {
			return err
		}

		tx, err := db.Beginx()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(string(b)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", v, err)
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations(version) VALUES($1)", v); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
