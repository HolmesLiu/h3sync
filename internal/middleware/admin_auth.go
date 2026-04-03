package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

func RequireAdminLogin(validateUser func(string) (bool, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		sess := sessions.Default(c)
		user := sess.Get("admin_user")
		if user == nil {
			c.Redirect(http.StatusFound, "/admin/login")
			c.Abort()
			return
		}
		username, ok := user.(string)
		if !ok || strings.TrimSpace(username) == "" {
			sess.Clear()
			_ = sess.Save()
			c.Redirect(http.StatusFound, "/admin/login")
			c.Abort()
			return
		}
		if validateUser != nil {
			valid, err := validateUser(username)
			if err != nil || !valid {
				sess.Clear()
				_ = sess.Save()
				c.Redirect(http.StatusFound, "/admin/login")
				c.Abort()
				return
			}
		}
		c.Set("admin_user", username)
		c.Next()
	}
}
