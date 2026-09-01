package http

import "net/http"

type Middleware func(http.Handler) http.Handler
type MiddlewareFunc func(http.HandlerFunc) http.HandlerFunc

func Chain(handler http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		handler = mw[i](handler)
	}

	return handler
}
