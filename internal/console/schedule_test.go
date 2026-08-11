package console

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zepto-labs/scherry/internal/jobconfig"
)

func TestHandleConfig(t *testing.T) {
	svc := &mockScheduler{jobs: map[string]jobconfig.JobConfig{
		"job-b": {Name: "job-b", Schedule: "0 * * * *", Kafka: jobconfig.KafkaConfig{Brokers: []string{"b:9092"}, TaskTopic: "topic"}},
		"job-a": {Name: "job-a", Schedule: "not-a-cron", Kafka: jobconfig.KafkaConfig{TaskTopic: "t2"}},
		"manual": {Name: "manual", Schedule: ""},
	}}
	handler := NewHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `"manual":true`)
	assert.Contains(t, body, "parse_error")
	assert.Contains(t, body, "job-a")
}

func TestHandleUpcoming(t *testing.T) {
	svc := &mockScheduler{jobs: map[string]jobconfig.JobConfig{
		"hourly": {Name: "hourly", Schedule: "0 * * * *"},
		"bad":    {Name: "bad", Schedule: "invalid cron"},
	}}
	handler := NewHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/upcoming?count=2", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"runs"`)
	assert.Contains(t, rec.Body.String(), "hourly")
}
