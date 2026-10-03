package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"summary-orchestrator/internal/config"
	audio_store "summary-orchestrator/pkg/audio-store"
	session_lookup "summary-orchestrator/pkg/session-lookup"
	"summary-orchestrator/pkg/storyteller"
	summary_store "summary-orchestrator/pkg/summary-store"
	"summary-orchestrator/pkg/transcriber"
	whisper_lease "summary-orchestrator/pkg/whisper-lease"
	"summary-orchestrator/services/api"
	"summary-orchestrator/workflows"

	"github.com/dapr/durabletask-go/workflow"
	"github.com/dapr/go-sdk/client"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The agent writes a whole chapter in one call, which is minutes of model
// time, and it waits for Graphiti to ingest the result before answering.
const agentTimeout = 30 * time.Minute

// Queueing a transcription is not the quick call it looks like: whisper
// downloads and decodes the whole recording inline before it answers, so this
// has to cover transferring a full session, not just an HTTP round trip.
const whisperTimeout = 15 * time.Minute

func main() {
	cfg := config.Parse()
	slog.Info(fmt.Sprintf("[Main] :: %s %s starting", cfg.ServiceName, cfg.ServiceVersion))

	daprClient, err := makeDaprClient(cfg.DaprGrpcPort)
	if err != nil {
		slog.Error(fmt.Sprintf("[Main] :: could not reach the Dapr sidecar : %s", err))
		os.Exit(1)
	}
	defer daprClient.Close()

	wfClient, err := client.NewWorkflowClient()
	if err != nil {
		slog.Error(fmt.Sprintf("[Main] :: could not create the workflow client : %s", err))
		os.Exit(1)
	}

	whisper := transcriber.New(transcriber.Config{
		BaseUrl:     cfg.WhisperUrl,
		Token:       cfg.WhisperToken,
		Language:    cfg.WhisperLanguage,
		Model:       cfg.WhisperModel,
		Device:      cfg.WhisperDevice,
		ComputeType: cfg.WhisperComputeType,
	}, &http.Client{Timeout: whisperTimeout})

	summaries := summary_store.New(cfg.SummaryStore, daprClient)

	registry := workflow.NewRegistry()
	err = workflows.Register(registry, workflows.Deps{
		Audio:       audio_store.New(cfg.ObjectStore, cfg.PresignTTL, daprClient),
		Transcriber: whisper,
		Whisper:     whisper_lease.New(cfg.StateStore, daprClient, whisper.Status),
		Agent:       storyteller.New(cfg.AgentEndpoint(), cfg.GatewayKey, cfg.StorytellerAgent, cfg.StorytellerViaGateway, &http.Client{Timeout: agentTimeout}),
		Summaries:   summaries,
		Sessions:    session_lookup.New(cfg.Db, daprClient),
		Publisher:   daprClient,
		PubSubName:  cfg.PubSub,
		GistTopic:   cfg.GistTopic,
	})
	if err != nil {
		slog.Error(fmt.Sprintf("[Main] :: could not register the workflow : %s", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := wfClient.StartWorker(ctx, registry); err != nil {
		slog.Error(fmt.Sprintf("[Main] :: could not start the workflow worker : %s", err))
		os.Exit(1)
	}
	slog.Info("[Main] :: workflow worker started")

	r := gin.Default()
	api.NewController(wfClient, summaries).Register(r)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.AppPort),
		Handler: r,
	}
	go func() {
		slog.Info(fmt.Sprintf("[Main] :: listening on 0.0.0.0:%d", cfg.AppPort))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error(fmt.Sprintf("[Main] :: server stopped : %s", err))
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("[Main] :: shutting down")

	// Give in-flight requests a moment. Workflows themselves are durable, so
	// anything mid-run is picked back up on the next start.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn(fmt.Sprintf("[Main] :: unclean shutdown : %s", err))
	}
}

func makeDaprClient(port int) (client.Client, error) {
	conn, err := grpc.NewClient(
		net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return client.NewClientWithConnection(conn), nil
}
