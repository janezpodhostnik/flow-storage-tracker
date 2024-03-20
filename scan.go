package flow_storage_tracker

import (
	"context"
	_ "embed"
	"os"
	"os/signal"
	"syscall"

	flowModel "github.com/onflow/flow-go/model/flow"
	executiondata "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/onflow/flow-batch-scan/client/interceptors"

	"github.com/onflow/flow-go-sdk"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	scanner "github.com/onflow/flow-batch-scan"
	"github.com/onflow/flow-batch-scan/candidates"
	"github.com/onflow/flow-batch-scan/client"
)

//go:embed cadence/script.cdc
var GetPublicVaults string

func Scan(flowAccessNodeURL string, batchSize int, connectionString string) {
	log.Logger = log.
		Output(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(zerolog.InfoLevel)

	flowClient, exeDataClient, c := createClients(flowAccessNodeURL, log.Logger)
	defer c()

	ctx, cancel := setupContextAndSignalHandling(log.Logger)
	defer cancel()

	reporter := NewReporter(
		log.Logger,
	)

	db, err := NewPGDB(
		connectionString,
		log.Logger,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create db")
	}
	defer func() {
		err = db.Close()
		if err != nil {
			log.Warn().Err(err).Msg("failed to close db")
		}
	}()

	scriptResultHandler := NewScriptResultHandler(
		db,
		reporter,
		log.Logger,
	)

	candidateScanners := []candidates.CandidateScanner{
		NewCandidateScanner(flowModel.Mainnet.Chain(), exeDataClient, log.Logger),
	}

	script := []byte(GetPublicVaults)

	config := scanner.DefaultConfig().
		WithScript(script).
		WithScriptResultHandler(scriptResultHandler).
		WithCandidateScanners(candidateScanners).
		WithBatchSize(batchSize).
		WithChainID(flow.Mainnet).
		WithStatusReporter(reporter).
		WithLogger(log.Logger).
		WithContinuousScan(true)

	scan := scanner.NewScanner(
		flowClient,
		config,
	)

	_, err = scan.Scan(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("scanner failed")
	}
}

func setupContextAndSignalHandling(logger zerolog.Logger) (context.Context, func()) {
	ctx := context.Background()

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	osc := make(chan os.Signal, 1)
	signal.Notify(osc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGHUP,
	)
	go func() {
		select {
		case s := <-osc:
			logger.Warn().
				Str("signal", s.String()).
				Msg("received signal, shutting down")
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(osc)
	}()

	return ctx, cancel
}

func createClients(flowAccessNodeURL string, logger zerolog.Logger) (
	client.Client,
	executiondata.ExecutionDataAPIClient,
	func(),
) {
	grpcMetrics := client.DefaultClientMetrics("flow_storage_tracker")

	flowConn, err := client.NewConnection(flowAccessNodeURL,
		func(config *client.Config) {
			config.Log = logger
			config.ClientMetrics = grpcMetrics
		})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create client")
	}

	flowClient := client.NewClientFromConnection(flowConn)

	conn, err := grpc.Dial(
		flowAccessNodeURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)),
		grpc.WithChainUnaryInterceptor(
			interceptors.UnpackCancelledUnaryClientInterceptor(),
			interceptors.LogUnaryClientInterceptor(logger),
			interceptors.RetryUnaryClientInterceptor(3),
			interceptors.RateLimitUnaryClientInterceptor(
				2,
				map[string]int{
					"/flow.access.ExecutionDataAPI/GetExecutionDataByBlockID": 2,
				},
				logger,
			),
			grpcMetrics.UnaryClientInterceptor(),
		),
	)
	if err != nil {
		logger.Fatal().Msgf("could not connect to exec data api server: %v", err)
	}

	execClient := executiondata.NewExecutionDataAPIClient(conn)

	return flowClient, execClient, func() {
		err := flowConn.Close()
		if err != nil {
			log.Error().Err(err).Msg("failed to close client")
		}
		err = conn.Close()
		if err != nil {
			log.Error().Err(err).Msg("failed to close execution data client")
		}
	}
}
