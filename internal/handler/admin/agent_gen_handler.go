package admin

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func (h Handlers) agentGenPage(c *gin.Context) {
	roles, err := h.AgentRepo.ListRoles()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	
	keys, _ := h.FormRepo.ListAPIKeys()
	
	c.HTML(http.StatusOK, "agent_gen.tmpl", gin.H{
		"navItem": "agent-gen",
		"title":   "Agent 生成与配置中心",
		"roles":   roles,
		"keys":    keys,
		"message": c.Query("msg"),
		"host":    c.Request.Host,
	})
}

func (h Handlers) createAgentRole(c *gin.Context) {
	name := strings.TrimSpace(c.PostForm("name"))
	content := strings.TrimSpace(c.PostForm("content"))
	
	if name == "" {
		c.Redirect(http.StatusFound, "/admin/agent-gen?msg=角色名不能为空")
		return
	}
	
	if err := h.AgentRepo.CreateRole(name, content); err != nil {
		c.Redirect(http.StatusFound, "/admin/agent-gen?msg=保存角色失败: "+err.Error())
		return
	}
	
	c.Redirect(http.StatusFound, "/admin/agent-gen?msg=角色已保存")
}

func (h Handlers) deleteAgentRole(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.ParseInt(idStr, 10, 64)
	if id > 0 {
		_ = h.AgentRepo.DeleteRole(id)
	}
	c.Redirect(http.StatusFound, "/admin/agent-gen?msg=角色已删除")
}
