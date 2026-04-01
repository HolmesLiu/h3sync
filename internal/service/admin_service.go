package service

import (
	"database/sql"

	"github.com/HolmesLiu/h3sync/internal/auth"
	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/repository"
)

type AdminService struct {
	users *repository.AdminUserRepo
}

func NewAdminService(users *repository.AdminUserRepo) *AdminService {
	return &AdminService{users: users}
}

func (s *AdminService) Bootstrap(username, password string) error {
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	return s.users.EnsureBootstrapUser(username, hash)
}

func (s *AdminService) Login(username, password string) (int64, error) {
	id, hash, err := s.users.GetByUsername(username)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, sql.ErrNoRows
		}
		return 0, err
	}
	if err := auth.ComparePassword(hash, password); err != nil {
		return 0, sql.ErrNoRows
	}
	return id, nil
}

func (s *AdminService) Audit(log models.AdminAuditLog) {
	_ = s.users.AddAuditLog(log)
}
