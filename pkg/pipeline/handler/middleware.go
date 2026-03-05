package handler

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

func SecretMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Bot-Runtime-Secret") != secret {
			slog.Warn("pipeline.auth.unauthorized",
				"remote_addr", c.ClientIP(),
			)
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "unauthorized",
				"code":  "ERR_UNAUTHORIZED",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
