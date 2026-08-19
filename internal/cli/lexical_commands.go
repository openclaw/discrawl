package cli

import (
	"errors"
	"fmt"

	"github.com/openclaw/discrawl/internal/store"
)

type lexicalInstallOutput struct {
	Languages []string `json:"languages"`
	Packages  []string `json:"packages"`
	Python    string   `json:"python"`
	Kiwi      string   `json:"kiwi,omitempty"`
}

func (r *runtime) runLexical(args []string) error {
	if len(args) != 1 || args[0] != "install" {
		return usageErr(errors.New("usage: discrawl lexical install"))
	}
	if len(r.cfg.Search.Lexical.Languages) == 0 {
		return configErr(errors.New("search.lexical.languages is empty"))
	}
	install := r.installLexical
	if install == nil {
		install = store.InstallLexicalPackages
	}
	result, err := install(
		r.ctx,
		r.cfg.Search.Lexical.Python,
		r.cfg.Search.Lexical.Languages,
	)
	if err != nil {
		return configErr(fmt.Errorf("install lexical tokenizers: %w", err))
	}
	return r.print(lexicalInstallOutput{
		Languages: append([]string(nil), r.cfg.Search.Lexical.Languages...),
		Packages:  append([]string{}, result.Packages...),
		Python:    r.cfg.Search.Lexical.Python,
		Kiwi:      r.cfg.Search.Lexical.KiwiCommand,
	})
}
