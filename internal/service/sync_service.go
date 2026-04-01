package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/HolmesLiu/h3sync/internal/connector/h3"
	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/HolmesLiu/h3sync/internal/repository"
	"go.uber.org/zap"
)

type SyncService struct {
	formRepo  *repository.FormRepo
	h3Client  *h3.Client
	pageSize  int
	logger    *zap.Logger
}

func NewSyncService(formRepo *repository.FormRepo, h3Client *h3.Client, pageSize int, logger *zap.Logger) *SyncService {
	return &SyncService{formRepo: formRepo, h3Client: h3Client, pageSize: pageSize, logger: logger}
}

func (s *SyncService) RunAutoOnce(ctx context.Context) {
	forms, err := s.formRepo.ListEnabledAutoDue(time.Now().UTC())
	if err != nil {
		s.logger.Error("load due forms failed", zap.Error(err))
		return
	}
	for _, f := range forms {
		if err := s.SyncForm(ctx, f, "AUTO"); err != nil {
			s.logger.Error("sync form failed", zap.String("schema", f.SchemaCode), zap.Error(err))
		}
	}
}

func (s *SyncService) SyncBySchema(ctx context.Context, schemaCode string, trigger string) error {
	form, err := s.formRepo.GetBySchema(schemaCode)
	if err != nil {
		return err
	}
	return s.SyncForm(ctx, form, trigger)
}

func (s *SyncService) SyncForm(ctx context.Context, form models.FormRegistry, trigger string) error {
	logID, err := s.formRepo.InsertSyncLog(form.ID, trigger)
	if err != nil {
		return err
	}

	cursorBefore := ""
	if form.LastCursorModifiedTime != nil {
		cursorBefore = form.LastCursorModifiedTime.Format(time.RFC3339)
	}

	count, lastModified, lastObjectID, runErr := s.doSync(ctx, form)
	cursorAfter := ""
	if lastModified != nil {
		cursorAfter = lastModified.Format(time.RFC3339)
	}
	if lastObjectID != nil {
		cursorAfter = cursorAfter + "#" + *lastObjectID
	}

	if runErr != nil {
		msg := runErr.Error()
		_ = s.formRepo.FinishSyncLog(logID, "FAILED", count, &msg, &cursorBefore, &cursorAfter)
		return runErr
	}

	_ = s.formRepo.FinishSyncLog(logID, "SUCCESS", count, nil, &cursorBefore, &cursorAfter)
	return nil
}

func (s *SyncService) doSync(ctx context.Context, form models.FormRegistry) (int, *time.Time, *string, error) {
	fields, err := s.h3Client.GetFormFields(ctx, form.SchemaCode)
	if err != nil {
		return 0, nil, nil, err
	}
	columns := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.Code == "" {
			continue
		}
		columns = append(columns, f.Code)
		_ = s.formRepo.UpsertFieldRemark(form.ID, models.FormFieldRegistry{
			FormID:        form.ID,
			FieldCode:     f.Code,
			FieldName:     f.Name,
			ChineseRemark: "",
			ShowInAdmin:   true,
			OriginalType:  f.Type,
			StorageType:   "text",
		})
	}
	sort.Strings(columns)

	if err := s.formRepo.EnsureBizTable(form.SchemaCode, columns); err != nil {
		return 0, nil, nil, err
	}

	var synced int
	var lastModified *time.Time
	var lastObjectID *string
	page := 1

	for {
		items, err := s.h3Client.LoadBizObjects(ctx, form.SchemaCode, page, s.pageSize, form.LastCursorModifiedTime)
		if err != nil {
			return synced, lastModified, lastObjectID, err
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			if item.ObjectID == "" {
				continue
			}
			raw, _ := json.Marshal(item.Data)
			flat := flatten(item.Data)
			if err := s.formRepo.UpsertBizRow(form.SchemaCode, item.ObjectID, item.ModifiedTime, string(raw), flat); err != nil {
				return synced, lastModified, lastObjectID, err
			}
			synced++
			if item.ModifiedTime != nil {
				lastModified = item.ModifiedTime
				id := item.ObjectID
				lastObjectID = &id
			}
		}
		page++
	}

	if err := s.formRepo.UpdateCursor(form.ID, lastModified, lastObjectID); err != nil {
		return synced, lastModified, lastObjectID, fmt.Errorf("update cursor: %w", err)
	}
	return synced, lastModified, lastObjectID, nil
}

func flatten(input map[string]any) map[string]string {
	out := map[string]string{}
	for k, v := range input {
		switch tv := v.(type) {
		case nil:
			out[k] = ""
		case string:
			out[k] = tv
		default:
			b, _ := json.Marshal(tv)
			out[k] = string(b)
		}
	}
	return out
}
