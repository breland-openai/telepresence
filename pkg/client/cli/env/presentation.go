package env

import "github.com/spf13/pflag"

const Redacted = "[REDACTED]"

func AddShowFlag(flags *pflag.FlagSet, show *bool) {
	flags.BoolVar(show, "show-env", false, "Include remote environment values in output. WARNING: may expose sensitive values")
}

// Presentation copies an environment for output without changing runtime values.
func Presentation(values map[string]string, show bool) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		if !show {
			value = Redacted
		}
		result[key] = value
	}
	return result
}
