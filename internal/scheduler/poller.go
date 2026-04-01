package scheduler

import (
	"context"
	"time"

	"github.com/HolmesLiu/h3sync/internal/service"
	"go.uber.org/zap"
)

type Poller struct {
	syncService *service.SyncService
	interval    time.Duration
	logger      *zap.Logger
}

func NewPoller(syncService *service.SyncService, intervalSeconds int, logger *zap.Logger) *Poller {
	if intervalSeconds <= 0 {
		intervalSeconds = 30
	}
	return &Poller{syncService: syncService, interval: time.Duration(intervalSeconds) * time.Second, logger: logger}
}

func (p *Poller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.logger.Info("sync poller started", zap.Duration("interval", p.interval))
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("sync poller stopped")
			return
		case <-ticker.C:
			p.syncService.RunAutoOnce(ctx)
		}
	}
}
