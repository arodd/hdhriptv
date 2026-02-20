package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// BasicAuth enforces HTTP basic auth when credential is configured as "user:pass".
func BasicAuth(credential string) func(http.Handler) http.Handler {
	credential = strings.TrimSpace(credential)
	if credential == "" {
		return func(next http.Handler) http.Handler { return next }
	}

	user, pass, ok := strings.Cut(credential, ":")
	if !ok || strings.TrimSpace(user) == "" || strings.TrimSpace(pass) == "" {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "invalid admin auth configuration", http.StatusInternalServerError)
			})
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rUser, rPass, hasAuth := r.BasicAuth()
			if !hasAuth || !secureCompare(rUser, user) || !secureCompare(rPass, pass) {
				w.Header().Set("WWW-Authenticate", `Basic realm="hdhriptv-admin"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func secureCompare(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
