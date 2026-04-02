package service

import (
	"database/sql"
	"time"

	"github.com/HolmesLiu/h3sync/internal/auth"
	"github.com/HolmesLiu/h3sync/internal/repository"
)

type APIKeyService struct {
	formRepo *repository.FormRepo
}

func NewAPIKeyService(formRepo *repository.FormRepo) *APIKeyService {
	return &APIKeyService{formRepo: formRepo}
}

func (s *APIKeyService) Create(name, remark string, expiresAt *time.Time, schemaCodes []string) (string, error) {
	plain, prefix, hash, err := auth.NewAPIKey()
	if err != nil {
		return "", err
	}

	var exp *time.Time
	if expiresAt != nil {
		exp = expiresAt
	}

	if _, err := s.formRepo.CreateAPIKey(name, remark, hash, prefix, plain, exp, schemaCodes); err != nil {
		return "", err
	}

	return plain, nil
}

func (s *APIKeyService) Update(keyID int64, name, remark string, expiresAt *time.Time, schemaCodes []string) error {
	return s.formRepo.UpdateAPIKey(keyID, name, remark, expiresAt, schemaCodes)
}

func (s *APIKeyService) Delete(keyID int64) error {
	return s.formRepo.DeleteAPIKey(keyID)
}

func (s *APIKeyService) ValidateForForm(plainKey string, schemaCode string) (int64, error) {
	hash := auth.HashAPIKey(plainKey)
	ak, err := s.formRepo.ValidateAPIKeyAndForm(hash, schemaCode)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, sql.ErrNoRows
		}
		return 0, err
	}
	return ak.ID, nil
}
