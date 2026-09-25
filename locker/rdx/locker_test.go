package rdx

import (
	"testing"

	"github.com/go-sdk/core/testx"
)

func TestNewRequiresClient(t *testing.T) {
	locker, err := New(nil, Config{})
	testx.Nil(t, locker)
	testx.ErrorIs(t, err, ErrClientRequired)
}

func TestNormalizeConfig(t *testing.T) {
	config := normalizeConfig(Config{})
	testx.Equal(t, defaultKeyPrefix, config.KeyPrefix)
	testx.Equal(t, defaultExpiry, config.Expiry)
	testx.Equal(t, defaultAutoExtendEvery, config.AutoExtendEvery)
	testx.Equal(t, defaultTries, config.Tries)

	config = normalizeConfig(Config{KeyPrefix: "certops"})
	testx.Equal(t, "certops:", config.KeyPrefix)
}
