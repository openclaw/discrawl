package store

import "strings"

type LexicalHelperStatus struct {
	Language string `json:"language"`
	Runtime  string `json:"runtime"`
	Command  string `json:"command,omitempty"`
}

func LexicalHelperStatuses(opts OpenOptions) []LexicalHelperStatus {
	statuses := make([]LexicalHelperStatus, 0, len(opts.LexicalLanguages))
	for _, language := range opts.LexicalLanguages {
		switch language {
		case "ko":
			command := strings.TrimSpace(opts.LexicalKiwiCommand)
			if command == "" {
				command = "discrawl-kiwi"
			}
			statuses = append(statuses, LexicalHelperStatus{
				Language: language,
				Runtime:  "helper",
				Command:  command,
			})
		case "ja":
			command := strings.TrimSpace(opts.LexicalJaCommand)
			if command == "" {
				command = "discrawl-ja"
			}
			statuses = append(statuses, LexicalHelperStatus{
				Language: language,
				Runtime:  "helper",
				Command:  command,
			})
		case "zh":
			command := strings.TrimSpace(opts.LexicalZhCommand)
			if command == "" {
				command = "discrawl-zh"
			}
			statuses = append(statuses, LexicalHelperStatus{
				Language: language,
				Runtime:  "helper",
				Command:  command,
			})
		case "ar":
			statuses = append(statuses, LexicalHelperStatus{
				Language: language,
				Runtime:  "in-process",
			})
		}
	}
	return statuses
}
