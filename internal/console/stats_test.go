package console

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResolveBucket(t *testing.T) {
	t.Run("explicit bucket", func(t *testing.T) {
		assert.Equal(t, "hour", resolveBucket("hour", nil, nil))
	})

	t.Run("auto minute for short range", func(t *testing.T) {
		from := time.Now().Add(-time.Hour)
		assert.Equal(t, "minute", resolveBucket("", &from, nil))
	})

	t.Run("auto hour for medium range", func(t *testing.T) {
		from := time.Now().Add(-24 * time.Hour)
		assert.Equal(t, "hour", resolveBucket("", &from, nil))
	})

	t.Run("defaults to day", func(t *testing.T) {
		assert.Equal(t, "day", resolveBucket("invalid", nil, nil))
	})
}

func TestCreatedAtRange(t *testing.T) {
	from := time.Now().Add(-time.Hour)
	to := time.Now()
	conds, args := createdAtRange([]string{"1 = 1"}, nil, &from, &to)
	assert.Len(t, conds, 3)
	assert.Len(t, args, 2)
}

func TestOutcomeFilters(t *testing.T) {
	succeeded, failed, other := outcomeFilters()
	assert.Contains(t, succeeded, "succeeded")
	assert.Contains(t, failed, "failed")
	assert.Contains(t, other, "other")
}
