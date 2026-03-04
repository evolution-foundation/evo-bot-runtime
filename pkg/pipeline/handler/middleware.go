package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func SecretMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Bot-Runtime-Secret") != secret {
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
