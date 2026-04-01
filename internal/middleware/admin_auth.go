package middleware

import (
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

func RequireAdminLogin() gin.HandlerFunc {
	return func(c *gin.Context) {
		sess := sessions.Default(c)
		user := sess.Get("admin_user")
		if user == nil {
			c.Redirect(http.StatusFound, "/admin/login")
			c.Abort()
			return
		}
		c.Set("admin_user", user.(string))
		c.Next()
	}
}
