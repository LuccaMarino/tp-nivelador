package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

const defaultBatchSize = 32 // si no se setea BATCH_SIZE, se usa este valor por default

func loadConfig() (client.ClientConfig, error) {
	agencyIdEnv := os.Getenv("AGENCY_ID")
	if agencyIdEnv == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	agencyId, err := strconv.ParseUint(agencyIdEnv, 10, 16)
	if err != nil {
		return client.ClientConfig{}, fmt.Errorf("invalid AGENCY_ID %q: %w", agencyIdEnv, err)
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSize := defaultBatchSize
	if batchSizeEnv := os.Getenv("BATCH_SIZE"); batchSizeEnv != "" {
		parsedBatchSize, err := strconv.Atoi(batchSizeEnv)
		if err != nil || parsedBatchSize <= 0 {
			return client.ClientConfig{}, fmt.Errorf("invalid BATCH_SIZE %q", batchSizeEnv)
		}
		batchSize = parsedBatchSize
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   uint16(agencyId),
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSize,
	}, nil
}

func run() int {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)

	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	agencyClient, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	go func() {
		<-signals
		logger.Info("sigterm", logger.InProgress)
		agencyClient.Shutdown()
	}()

	if err := agencyClient.Run(); err != nil {
		if errors.Is(err, client.ErrShutdown) {
			logger.Info("sigterm", logger.Success)
			return 0
		}
		logger.Error("client-run", logger.Fail, "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
