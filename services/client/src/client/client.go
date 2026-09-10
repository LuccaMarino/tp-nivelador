package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const connectionAttemptsMax = 3
const connectionAttemptsDelayMs = 200

const (
	betFieldsCount    = 5
	betFieldFirstName = 0
	betFieldLastName  = 1
	betFieldDocument  = 2
	betFieldBirthdate = 3
	betFieldNumber    = 4
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   uint16
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range connectionAttemptsMax {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

// parsea una linea del archivo de entrada en un Bet
func parseBet(line string) (bet.Bet, error) {
	fields := strings.Split(line, ",")
	if len(fields) != betFieldsCount {
		return bet.Bet{}, fmt.Errorf("expected %d fields, got %d", betFieldsCount, len(fields))
	}

	document, err := strconv.ParseUint(fields[betFieldDocument], 10, 32)
	if err != nil {
		return bet.Bet{}, fmt.Errorf("invalid document %q: %w", fields[betFieldDocument], err)
	}

	number, err := strconv.ParseUint(fields[betFieldNumber], 10, 32)
	if err != nil {
		return bet.Bet{}, fmt.Errorf("invalid number %q: %w", fields[betFieldNumber], err)
	}

	return bet.Bet{
		FirstName: fields[betFieldFirstName],
		LastName:  fields[betFieldLastName],
		Document:  uint32(document),
		Birthdate: fields[betFieldBirthdate],
		Number:    uint32(number),
	}, nil
}

func (client *Client) awaitAck() error {
	messageType, payload, err := protocol.ReceiveMessage(client.conn)
	if err != nil {
		return err
	}

	switch messageType {
	case protocol.MessageTypeAck:
		return nil
	case protocol.MessageTypeError:
		return fmt.Errorf("received error from server: %s", protocol.DeserializeErrorMessage(payload))
	default:
		return fmt.Errorf("expected an ack message, got %d", messageType)
	}
}

func (client *Client) sendBatch(batch []bet.Bet) error {
	if err := protocol.SendBets(client.conn, batch); err != nil {
		return err
	}
	return client.awaitAck() // espera a que
}

func (client *Client) sendBets(inputFile *os.File) (int, error) {
	const action = "send-bets"

	scanner := bufio.NewScanner(inputFile)
	batch := make([]bet.Bet, 0, client.config.BatchSize)
	betsCount := 0

	for scanner.Scan() {
		betLine := scanner.Text()
		if betLine == "" {
			continue
		}

		parsedBet, err := parseBet(betLine)
		if err != nil {
			logger.Error("parse-bet", logger.Fail, "bet-id", betsCount+1, "err", err)
			return betsCount, err
		}

		batch = append(batch, parsedBet)
		if len(batch) < client.config.BatchSize {
			continue
		}

		if err := client.sendBatch(batch); err != nil {
			logger.Error(action, logger.Fail, "bet-id", betsCount+1, "err", err)
			return betsCount, err
		}

		betsCount += len(batch)
		batch = batch[:0]
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "err", err)
		return betsCount, err
	}

	if len(batch) > 0 {
		if err := client.sendBatch(batch); err != nil {
			logger.Error(action, logger.Fail, "bets-count", betsCount, "err", err)
			return betsCount, err
		}
		betsCount += len(batch)
	}
	return betsCount, nil
}

func (client *Client) receiveWinners(outputFile *os.File) (int, error) {
	messageType, payload, err := protocol.ReceiveMessage(client.conn)
	if err != nil {
		return 0, err
	}

	switch messageType {
	case protocol.MessageTypeWinners:
	case protocol.MessageTypeError:
		return 0, fmt.Errorf("received error from server: %s", protocol.DeserializeErrorMessage(payload))
	default:
		return 0, fmt.Errorf("expected a winners message, got %d", messageType)
	}

	winners, err := protocol.DeserializeWinners(payload)
	if err != nil {
		return 0, err
	}

	writer := bufio.NewWriter(outputFile)
	for _, winner := range winners {
		_, err := fmt.Fprintf(writer, "%s,%s,%d,%s,%d\n",
			winner.FirstName, winner.LastName, winner.Document, winner.Birthdate, winner.Number)
		if err != nil {
			return 0, err
		}
	}

	if err := writer.Flush(); err != nil {
		return 0, err
	}
	return len(winners), nil
}

func (client *Client) Run() error {
	const action = "process-bets"
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "err", err)
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "err", err)
		return err
	}
	defer outputFile.Close()

	logger.Info(action, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := protocol.SendAgency(client.conn, client.config.AgencyId); err != nil {
		logger.Error("send-agency", logger.Fail, "err", err)
		return err
	}

	betsCount, err := client.sendBets(inputFile)
	if err != nil {
		return err
	}

	if err := protocol.SendDone(client.conn); err != nil {
		logger.Error("send-done", logger.Fail, "err", err)
		return err
	}

	winnersCount, err := client.receiveWinners(outputFile)
	if err != nil {
		logger.Error("receive-winners", logger.Fail, "err", err)
		return err
	}

	logger.Info(action, logger.Success,
		"agency-id", client.config.AgencyId,
		"bets-count", betsCount,
		"winners-count", winnersCount)
	return nil
}
