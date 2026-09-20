package config

import (
	"os"
	"strconv"
)

const (
	DefaultAppPort      = 8080
	DefaultDaprGrpcPort = 50001

	DefaultObjectStore  = "object-store"
	DefaultSummaryStore = "summaries"
	DefaultDb           = "db"
	DefaultPubSub       = "pubsub"
	DefaultGistTopic    = "gists"

	DefaultWhisperUrl      = "http://whisper:8000"
	DefaultWhisperLanguage = "fr"
	// /speech-to-text-url ignores the server's own WHISPER_MODEL, DEVICE and
	// COMPUTE_TYPE env vars and reads them from query parameters instead,
	// defaulting to "tiny"/"cuda"/"float16". These have to match how the
	// deployment is actually configured or the task fails on a CPU-only
	// server — see transcriber.Config.
	DefaultWhisperModel       = "nyrahealth/faster_CrisperWhisper"
	DefaultWhisperDevice      = "cpu"
	DefaultWhisperComputeType = "int8"

	DefaultStorytellerUrl   = "http://storyteller:9999"
	DefaultGatewayUrl       = "http://ai-gateway.private:4000"
	DefaultStorytellerAgent = "Storyteller"

	DefaultPresignTTL     = "6h"
	DefaultServiceName    = "summary-orchestrator"
	DefaultServiceVersion = "0.3.4"
)

// Config is the whole runtime configuration, read once at startup.
//
// Every Dapr component name is overridable because the defaults here are the
// names used in the tabletop-records namespace, not the Dapr defaults.
type Config struct {
	AppPort      int
	DaprGrpcPort int

	// Dapr component names
	ObjectStore  string
	SummaryStore string
	Db           string
	PubSub       string
	GistTopic    string

	// whisperX-FastAPI
	WhisperUrl      string
	WhisperToken    string
	WhisperLanguage string
	// Sent on every request rather than left to the server. The endpoint's
	// own defaults are "tiny"/"cuda"/"float16", not the deployment's env.
	WhisperModel       string
	WhisperDevice      string
	WhisperComputeType string

	// The Storyteller agent, reached directly by default. It answers
	// JSON-RPC on "/" of this address.
	StorytellerUrl string
	// Reach the agent through the LiteLLM proxy instead. Only worth it from
	// outside the cluster, and it additionally requires the agent to be
	// registered in the proxy's agent_list.
	StorytellerViaGateway bool
	GatewayUrl            string
	GatewayKey            string
	StorytellerAgent      string

	// TTL of the presigned URL handed to whisper. Only needs to outlive the
	// download itself: whisper fetches the file inline while answering the
	// request, not later when the queued task runs.
	PresignTTL string

	// Observability
	OtelEndpoint   string
	ServiceName    string
	ServiceVersion string
}

func Parse() *Config {
	c := &Config{
		AppPort:            DefaultAppPort,
		DaprGrpcPort:       DefaultDaprGrpcPort,
		ObjectStore:        DefaultObjectStore,
		SummaryStore:       DefaultSummaryStore,
		Db:                 DefaultDb,
		PubSub:             DefaultPubSub,
		GistTopic:          DefaultGistTopic,
		WhisperUrl:         DefaultWhisperUrl,
		WhisperLanguage:    DefaultWhisperLanguage,
		WhisperModel:       DefaultWhisperModel,
		WhisperDevice:      DefaultWhisperDevice,
		WhisperComputeType: DefaultWhisperComputeType,
		StorytellerUrl:     DefaultStorytellerUrl,
		GatewayUrl:         DefaultGatewayUrl,
		StorytellerAgent:   DefaultStorytellerAgent,
		PresignTTL:         DefaultPresignTTL,
		ServiceName:        DefaultServiceName,
		ServiceVersion:     DefaultServiceVersion,
	}

	intEnv("APP_PORT", &c.AppPort)
	intEnv("DAPR_GRPC_PORT", &c.DaprGrpcPort)

	strEnv("OBJECT_STORE_NAME", &c.ObjectStore)
	strEnv("SUMMARY_STORE_NAME", &c.SummaryStore)
	strEnv("DB_NAME", &c.Db)
	strEnv("PUBSUB_NAME", &c.PubSub)
	strEnv("GIST_TOPIC", &c.GistTopic)

	strEnv("WHISPER_URL", &c.WhisperUrl)
	strEnv("WHISPER_BEARER_TOKEN", &c.WhisperToken)
	strEnv("WHISPER_LANGUAGE", &c.WhisperLanguage)
	strEnv("WHISPER_MODEL", &c.WhisperModel)
	strEnv("WHISPER_DEVICE", &c.WhisperDevice)
	strEnv("WHISPER_COMPUTE_TYPE", &c.WhisperComputeType)

	strEnv("STORYTELLER_URL", &c.StorytellerUrl)
	boolEnv("STORYTELLER_VIA_GATEWAY", &c.StorytellerViaGateway)
	strEnv("GATEWAY_URL", &c.GatewayUrl)
	strEnv("GATEWAY_API_KEY", &c.GatewayKey)
	strEnv("STORYTELLER_AGENT", &c.StorytellerAgent)

	strEnv("PRESIGN_TTL", &c.PresignTTL)

	strEnv("OTEL_EXPORTER_OTLP_ENDPOINT", &c.OtelEndpoint)
	strEnv("SERVICE_NAME", &c.ServiceName)
	strEnv("SERVICE_VERSION", &c.ServiceVersion)

	return c
}

func strEnv(key string, into *string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		*into = v
	}
}

// AgentEndpoint is the address the A2A client should talk to, which depends
// on whether the proxy is in the way.
func (c *Config) AgentEndpoint() string {
	if c.StorytellerViaGateway {
		return c.GatewayUrl
	}
	return c.StorytellerUrl
}

func boolEnv(key string, into *bool) {
	if v, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		*into = v
	}
}

func intEnv(key string, into *int) {
	if v, err := strconv.ParseInt(os.Getenv(key), 10, 32); err == nil && v != 0 {
		*into = int(v)
	}
}
