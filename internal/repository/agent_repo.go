package repository

import (
	"github.com/HolmesLiu/h3sync/internal/models"
	"github.com/jmoiron/sqlx"
)

type AgentRepo struct{ db *sqlx.DB }

func NewAgentRepo(db *sqlx.DB) *AgentRepo { return &AgentRepo{db: db} }

func (r *AgentRepo) ListRoles() ([]models.AgentRole, error) {
	var roles []models.AgentRole
	err := r.db.Select(&roles, `SELECT id, name, content, created_at FROM agent_roles ORDER BY name ASC`)
	return roles, err
}

func (r *AgentRepo) CreateRole(name, content string) error {
	_, err := r.db.Exec(`INSERT INTO agent_roles(name, content) VALUES($1, $2) ON CONFLICT (name) DO UPDATE SET content=EXCLUDED.content, updated_at=now()`, name, content)
	return err
}

func (r *AgentRepo) DeleteRole(id int64) error {
	_, err := r.db.Exec(`DELETE FROM agent_roles WHERE id=$1`, id)
	return err
}

func (r *AgentRepo) ListCoreRules() ([]models.AgentCoreRule, error) {
	var rules []models.AgentCoreRule
	err := r.db.Select(&rules, `SELECT id, name, content, created_at FROM agent_core_rules ORDER BY name ASC`)
	return rules, err
}

func (r *AgentRepo) CreateCoreRule(name, content string) error {
	_, err := r.db.Exec(`INSERT INTO agent_core_rules(name, content) VALUES($1, $2) ON CONFLICT (name) DO UPDATE SET content=EXCLUDED.content, updated_at=now()`, name, content)
	return err
}

func (r *AgentRepo) DeleteCoreRule(id int64) error {
	_, err := r.db.Exec(`DELETE FROM agent_core_rules WHERE id=$1`, id)
	return err
}
