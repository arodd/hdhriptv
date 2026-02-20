package ui

import (
	"embed"
	"html/template"
)

//go:embed templates/*.html
var templateFS embed.FS

func ParseTemplates() (*template.Template, error) {
	return template.ParseFS(templateFS, "templates/*.html")
}
