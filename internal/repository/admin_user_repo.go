package repository

import (
	"database/sql"
	"fmt"

	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/jmoiron/sqlx"
)

type AdminUserRepo struct {
	db *sqlx.DB
}

func NewAdminUserRepo(db *sqlx.DB) *AdminUserRepo { return &AdminUserRepo{db: db} }

func (r *AdminUserRepo) GetByUsername(username string) (id int64, passwordHash string, err error) {
	err = r.db.QueryRow(`SELECT id, password_hash FROM admin_users WHERE username=$1 AND is_active=true`, username).Scan(&id, &passwordHash)
	return
}

func (r *AdminUserRepo) IsActiveUsername(username string) (bool, error) {
	var exists int
	err := r.db.QueryRow(`SELECT 1 FROM admin_users WHERE username=$1 AND is_active=true`, username).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (r *AdminUserRepo) EnsureBootstrapUser(username, passwordHash string) error {
	_, err := r.db.Exec(`
	INSERT INTO admin_users(username, password_hash, is_active)
	VALUES($1,$2,true)
	ON CONFLICT (username) DO NOTHING;
	`, username, passwordHash)
	return err
}

func (r *AdminUserRepo) AddAuditLog(log models.AdminAuditLog) error {
	_, err := r.db.Exec(`
	INSERT INTO admin_audit_logs(username, action, target_type, target_id, detail, client_ip)
	VALUES($1,$2,$3,$4,$5,$6)
	`, log.Username, log.Action, log.TargetType, log.TargetID, log.Detail, log.ClientIP)
	return err
}

func (r *AdminUserRepo) ListAdminUsers() ([]models.AdminUser, error) {
	var users []models.AdminUser
	err := r.db.Select(&users, `SELECT id, username, password_hash, is_active, created_at, updated_at FROM admin_users ORDER BY id ASC`)
	return users, err
}

func (r *AdminUserRepo) CreateAdminUser(username, passwordHash string) error {
	_, err := r.db.Exec(`
	INSERT INTO admin_users(username, password_hash, is_active, created_at, updated_at)
	VALUES($1, $2, true, now(), now())
	`, username, passwordHash)
	return err
}

func (r *AdminUserRepo) UpdateAdminUserStatus(id int64, isActive bool) error {
	_, err := r.db.Exec(`UPDATE admin_users SET is_active=$1, updated_at=now() WHERE id=$2`, isActive, id)
	return err
}

func (r *AdminUserRepo) UpdateAdminUserPassword(id int64, passwordHash string) error {
	_, err := r.db.Exec(`UPDATE admin_users SET password_hash=$1, updated_at=now() WHERE id=$2`, passwordHash, id)
	return err
}

func (r *AdminUserRepo) DeleteAdminUser(id int64) error {
	_, err := r.db.Exec(`DELETE FROM admin_users WHERE id=$1`, id)
	return err
}

func (r *AdminUserRepo) CountAuditLogs(action, username string) (int, error) {
	var count int
	query := `SELECT count(*) FROM admin_audit_logs WHERE 1=1`
	args := []interface{}{}
	if action != "" {
		args = append(args, "%"+action+"%")
		query += fmt.Sprintf(` AND action ILIKE $%d`, len(args))
	}
	if username != "" {
		args = append(args, "%"+username+"%")
		query += fmt.Sprintf(` AND username ILIKE $%d`, len(args))
	}
	err := r.db.QueryRow(query, args...).Scan(&count)
	return count, err
}

func (r *AdminUserRepo) ListAuditLogs(limit, offset int, action, username string) ([]models.AdminAuditLog, error) {
	var logs []models.AdminAuditLog
	query := `SELECT id, username, action, target_type, target_id, detail, client_ip, created_at FROM admin_audit_logs WHERE 1=1`
	args := []interface{}{}
	if action != "" {
		args = append(args, "%"+action+"%")
		query += fmt.Sprintf(` AND action ILIKE $%d`, len(args))
	}
	if username != "" {
		args = append(args, "%"+username+"%")
		query += fmt.Sprintf(` AND username ILIKE $%d`, len(args))
	}
	
	args = append(args, limit, offset)
	query += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, len(args)-1, len(args))
	err := r.db.Select(&logs, query, args...)
	return logs, err
}
