package env

import (
	"github.com/spf13/pflag"
)

type Flags struct {
	Show   bool   // --show-env
	File   string // --env-file
	Syntax Syntax // --env-syntax
	JSON   string // --env-json
}

func (f *Flags) AddFlags(flagSet *pflag.FlagSet) {
	AddShowFlag(flagSet, &f.Show)
	flagSet.StringVarP(&f.File, "env-file", "e", "", ``+
		`Export remote environment values, including sensitive values, to a file ("-" for stdout). Explicit opt-in for this export only; see --env-syntax`)

	flagSet.Var(&f.Syntax, "env-syntax", `Syntax used for env-file. One of `+SyntaxUsage())

	flagSet.StringVarP(&f.JSON, "env-json", "j", "", `Export remote environment values, including sensitive values, as JSON ("-" for stdout). Explicit opt-in for this export only.`)
}

func (f *Flags) MaybeWrite(env map[string]string) error {
	if f.File != "" {
		if err := f.Syntax.writeFile(f.File, env); err != nil {
			return err
		}
	}
	if f.JSON != "" {
		if err := SyntaxJSON.writeFile(f.JSON, env); err != nil {
			return err
		}
	}
	return nil
}
