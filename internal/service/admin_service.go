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

func (s *AdminService) IsActiveUsername(username string) (bool, error) {
	return s.users.IsActiveUsername(username)
}

func (s *AdminService) Audit(log models.AdminAuditLog) {
	_ = s.users.AddAuditLog(log)
}

func (s *AdminService) ListAdminUsers() ([]models.AdminUser, error) {
	return s.users.ListAdminUsers()
}

func (s *AdminService) CreateAdminUser(username, password string) error {
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	return s.users.CreateAdminUser(username, hash)
}

func (s *AdminService) UpdateAdminUserStatus(id int64, isActive bool) error {
	return s.users.UpdateAdminUserStatus(id, isActive)
}

func (s *AdminService) UpdateAdminUserPassword(id int64, password string) error {
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	return s.users.UpdateAdminUserPassword(id, hash)
}

func (s *AdminService) DeleteAdminUser(id int64) error {
	return s.users.DeleteAdminUser(id)
}

func (s *AdminService) ListAuditLogs(page, size int, action, username string) ([]models.AdminAuditLog, int, error) {
	count, err := s.users.CountAuditLogs(action, username)
	if err != nil {
		return nil, 0, err
	}
	offset := (page - 1) * size
	if offset < 0 {
		offset = 0
	}
	logs, err := s.users.ListAuditLogs(size, offset, action, username)
	return logs, count, err
}
