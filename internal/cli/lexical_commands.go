package cli

import (
	"errors"

	"github.com/openclaw/discrawl/internal/store"
)

type lexicalInstallOutput struct {
	Languages []string                    `json:"languages"`
	Helpers   []store.LexicalHelperStatus `json:"helpers"`
}

func (r *runtime) runLexical(args []string) error {
	if len(args) != 1 || args[0] != "install" {
		return usageErr(errors.New("usage: discrawl lexical install"))
	}
	if len(r.cfg.Search.Lexical.Languages) == 0 {
		return configErr(errors.New("search.lexical.languages is empty"))
	}
	return r.print(lexicalInstallOutput{
		Languages: append([]string(nil), r.cfg.Search.Lexical.Languages...),
		Helpers: store.LexicalHelperStatuses(store.OpenOptions{
			LexicalLanguages:   r.cfg.Search.Lexical.Languages,
			LexicalKiwiCommand: r.cfg.Search.Lexical.KiwiCommand,
			LexicalKiwiModel:   r.cfg.Search.Lexical.KiwiModel,
			LexicalJaCommand:   r.cfg.Search.Lexical.JaCommand,
			LexicalZhCommand:   r.cfg.Search.Lexical.ZhCommand,
		}),
	})
}
