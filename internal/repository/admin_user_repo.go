package repository

import (
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
