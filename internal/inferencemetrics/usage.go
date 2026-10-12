package inferencemetrics

import (
	"encoding/json"
	"strconv"
)

// usageScanner visits only metadata keys. Strings and numbers in data/embedding
// are skipped without buffering. It never reads ahead of the provider decoder.
// Parsing limits affect telemetry only; the provider still gets every byte.
type usageScanner struct {
	stack    [64]jsonFrame
	depth    int
	lex      byte
	escape   bool
	keep     bool
	token    [128]byte
	n        int
	overflow bool
	disabled bool
	done     bool
	input    *uint64
	cached   *uint64
	tier     string
	model    string
}

type jsonFrame struct {
	kind  byte
	key   bool
	field string
	scope byte // r=root, u=usage, d=prompt_tokens_details; zero skips payload
}

func (s *usageScanner) feed(p []byte) {
	for _, c := range p {
		if s.disabled || s.done {
			return
		}
		if s.lex == '"' {
			if s.keep {
				s.append(c)
			}
			if s.escape {
				s.escape = false
				continue
			}
			if c == '\\' {
				s.escape = true
				continue
			}
			if c == '"' {
				s.stringEnd()
				s.lex = 0
			}
			continue
		}
		if s.lex == 'n' {
			if c != ',' && c != '}' && c != ']' && c != ' ' && c != '\n' && c != '\r' && c != '\t' {
				if s.keep {
					s.append(c)
				}
				continue
			}
			s.numberEnd()
			s.lex = 0
		}
		switch c {
		case ' ', '\n', '\r', '\t', ':':
		case '{', '[':
			if s.depth == len(s.stack) {
				s.disabled = true
				s.input = nil
				s.cached = nil
				return
			}
			var scope byte
			if s.depth == 0 && c == '{' {
				scope = 'r'
			}
			if s.depth > 0 && c == '{' {
				f := &s.stack[s.depth-1]
				if f.scope == 'r' && f.field == "usage" {
					scope = 'u'
					s.input = nil
					s.cached = nil
				}
				if f.scope == 'u' && f.field == "prompt_tokens_details" {
					scope = 'd'
					s.cached = nil
				}
			}
			s.stack[s.depth] = jsonFrame{kind: c, key: c == '{', scope: scope}
			s.depth++
		case '}', ']':
			if s.depth > 0 {
				s.depth--
				if s.depth == 0 {
					s.done = true
				}
			}
		case ',':
			if s.depth > 0 {
				f := &s.stack[s.depth-1]
				f.key = f.kind == '{'
				f.field = ""
			}
		case '"':
			s.lex = '"'
			s.n = 0
			s.overflow = false
			s.keep = false
			if s.depth > 0 {
				f := &s.stack[s.depth-1]
				s.keep = f.scope != 0 && (f.key || f.scope == 'r' && (f.field == "service_tier" || f.field == "model"))
			}
			if s.keep {
				s.append(c)
			}
		default:
			s.lex = 'n'
			s.n = 0
			s.overflow = false
			s.keep = s.numericField()
			if s.keep {
				s.append(c)
			}
		}
	}
}

func (s *usageScanner) append(c byte) {
	if s.n < len(s.token) {
		s.token[s.n] = c
		s.n++
	} else {
		s.overflow = true
	}
}

func (s *usageScanner) stringEnd() {
	if s.depth == 0 {
		return
	}
	f := &s.stack[s.depth-1]
	value := ""
	if s.keep && !s.overflow {
		_ = json.Unmarshal(s.token[:s.n], &value)
	}
	if f.key {
		f.field = value
		f.key = false
		s.clearField()
		return
	}
	if f.scope == 'r' && f.field == "service_tier" {
		s.tier = value
		if value == "" {
			s.tier = "unknown"
		}
	}
	if f.scope == 'r' && f.field == "model" {
		s.model = value
		if value == "" {
			s.model = "unknown"
		}
	}
}

func (s *usageScanner) clearField() {
	// Last occurrence wins, including a malformed/null replacement. Never carry
	// valid usage across a later invalid field of the same name.
	f := &s.stack[s.depth-1]
	switch {
	case f.scope == 'r' && f.field == "usage":
		s.input = nil
		s.cached = nil
	case f.scope == 'r' && f.field == "service_tier":
		s.tier = "unknown"
	case f.scope == 'r' && f.field == "model":
		s.model = "unknown"
	case f.scope == 'u' && f.field == "prompt_tokens":
		s.input = nil
	case f.scope == 'u' && f.field == "prompt_tokens_details":
		s.cached = nil
	case f.scope == 'd' && f.field == "cached_tokens":
		s.cached = nil
	}
}

func (s *usageScanner) numericField() bool {
	if s.depth == 0 {
		return false
	}
	f := s.stack[s.depth-1]
	return f.scope == 'u' && f.field == "prompt_tokens" || f.scope == 'd' && f.field == "cached_tokens"
}

func (s *usageScanner) numberEnd() {
	if !s.keep || s.overflow || s.depth == 0 {
		return
	}
	// Require an exact nonnegative JSON integer, bounded by the exact integer
	// range of Prometheus samples. Unknown and zero remain different states.
	t := s.token[:s.n]
	if len(t) == 0 || len(t) > 1 && t[0] == '0' {
		return
	}
	for _, c := range t {
		if c < '0' || c > '9' {
			return
		}
	}
	n, err := strconv.ParseUint(string(t), 10, 64)
	if err != nil || n > 1<<53-1 {
		return
	}
	f := s.stack[s.depth-1]
	if f.scope == 'u' && f.field == "prompt_tokens" {
		s.input = &n
	}
	if f.scope == 'd' && f.field == "cached_tokens" {
		s.cached = &n
	}
}
