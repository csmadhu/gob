package utils

import (
	"os"
)

// GetEnv returns the value fo env if not found reutrns defaultValue
func GetEnv(env string, defaultValue string) string {
	value := os.Getenv(env)
	if value == "" {
		return defaultValue
	}

	return value
}
