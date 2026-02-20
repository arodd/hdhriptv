package sqlite

import "strings"

const sqlLikeEscapeChar = '!'

func sqlLikeContainsPattern(raw string) string {
	return "%" + escapeSQLLikeLiteral(raw) + "%"
}

func escapeSQLLikeLiteral(raw string) string {
	if raw == "" {
		return ""
	}

	var out strings.Builder
	out.Grow(len(raw) + 2)
	for _, r := range raw {
		switch r {
		case sqlLikeEscapeChar, '%', '_':
			out.WriteRune(sqlLikeEscapeChar)
		}
		out.WriteRune(r)
	}
	return out.String()
}
