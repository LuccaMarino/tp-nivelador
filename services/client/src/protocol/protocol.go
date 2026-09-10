package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

type MessageType uint8

const (
	MessageTypeAgency  MessageType = 0x01
	MessageTypeBets    MessageType = 0x02
	MessageTypeDone    MessageType = 0x03
	MessageTypeAck     MessageType = 0x04
	MessageTypeWinners MessageType = 0x05
	MessageTypeError   MessageType = 0x06
)

const (
	typeSize         = 1
	lengthSize       = 4
	headerSize       = typeSize + lengthSize
	stringLengthSize = 1
	maxStringLength  = 1<<(8*stringLengthSize) - 1
	agencyIdSize     = 2
	documentSize     = 4
	numberSize       = 4
	birthdateSize    = 10
	winnersCountSize = 2
	betsCountSize    = 2
	maxBetsCount     = 1<<(8*betsCountSize) - 1
	maxPayloadSize   = 1024 * 1024
)

func deserializeUint16(data []byte, offset int) (uint16, int, error) {
	end := offset + 2
	if end > len(data) {
		return 0, 0, errors.New("payload too short to decode uint16")
	}
	return binary.BigEndian.Uint16(data[offset:end]), end, nil
}

func deserializeUint32(data []byte, offset int) (uint32, int, error) {
	end := offset + 4
	if end > len(data) {
		return 0, 0, errors.New("payload too short to decode uint32")
	}
	return binary.BigEndian.Uint32(data[offset:end]), end, nil
}

func serializeString(data []byte, value string) ([]byte, error) {
	if len(value) > maxStringLength {
		return nil, fmt.Errorf("string too long to serialize: %d bytes", len(value))
	}
	data = append(data, uint8(len(value)))
	return append(data, value...), nil
}

func deserializeString(data []byte, offset int) (string, int, error) {
	if offset+stringLengthSize > len(data) {
		return "", 0, errors.New("payload too short to decode string length")
	}
	length := int(data[offset])
	offset += stringLengthSize

	end := offset + length
	if end > len(data) {
		return "", 0, errors.New("payload too short to decode string")
	}
	return string(data[offset:end]), end, nil
}

func serializeFixedString(data []byte, value string, size int) ([]byte, error) {
	if len(value) != size {
		return nil, fmt.Errorf("unexpected fixed string size: %d bytes", len(value))
	}
	return append(data, value...), nil
}

func deserializeFixedString(data []byte, offset int, size int) (string, int, error) {
	end := offset + size
	if end > len(data) {
		return "", 0, errors.New("payload too short to decode fixed string")
	}
	return string(data[offset:end]), end, nil
}

func serializeBet(data []byte, b bet.Bet) ([]byte, error) {
	data, err := serializeString(data, b.FirstName)
	if err != nil {
		return nil, err
	}

	data, err = serializeString(data, b.LastName)
	if err != nil {
		return nil, err
	}

	data = binary.BigEndian.AppendUint32(data, b.Document)

	data, err = serializeFixedString(data, b.Birthdate, birthdateSize)
	if err != nil {
		return nil, err
	}

	return binary.BigEndian.AppendUint32(data, b.Number), nil
}

func deserializeBet(data []byte, offset int) (bet.Bet, int, error) {
	firstName, offset, err := deserializeString(data, offset)
	if err != nil {
		return bet.Bet{}, 0, err
	}

	lastName, offset, err := deserializeString(data, offset)
	if err != nil {
		return bet.Bet{}, 0, err
	}

	document, offset, err := deserializeUint32(data, offset)
	if err != nil {
		return bet.Bet{}, 0, err
	}

	birthdate, offset, err := deserializeFixedString(data, offset, birthdateSize)
	if err != nil {
		return bet.Bet{}, 0, err
	}

	number, offset, err := deserializeUint32(data, offset)
	if err != nil {
		return bet.Bet{}, 0, err
	}

	return bet.Bet{
		FirstName: firstName,
		LastName:  lastName,
		Document:  document,
		Birthdate: birthdate,
		Number:    number,
	}, offset, nil
}

func DeserializeWinners(payload []byte) ([]bet.Bet, error) {
	winnersCount, offset, err := deserializeUint16(payload, 0)
	if err != nil {
		return nil, err
	}

	winners := make([]bet.Bet, 0, winnersCount)
	for range int(winnersCount) {
		winner, newOffset, err := deserializeBet(payload, offset)
		if err != nil {
			return nil, err
		}
		offset = newOffset
		winners = append(winners, winner)
	}

	if offset != len(payload) {
		return nil, errors.New("unexpected payload length for winners")
	}
	return winners, nil
}

func SendMessage(socket io.Writer, messageType MessageType, payload []byte) error {
	if len(payload) > maxPayloadSize {
		return fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	message := make([]byte, 0, headerSize+len(payload))
	message = append(message, uint8(messageType))
	message = binary.BigEndian.AppendUint32(message, uint32(len(payload)))
	message = append(message, payload...)

	return safe_socket.SendAll(socket, message)
}

func SendAgency(socket io.Writer, agencyId uint16) error {
	payload := binary.BigEndian.AppendUint16(make([]byte, 0, agencyIdSize), agencyId)
	return SendMessage(socket, MessageTypeAgency, payload)
}

func SendBets(socket io.Writer, bets []bet.Bet) error {
	if len(bets) > maxBetsCount {
		return fmt.Errorf("too many bets to serialize: %d", len(bets))
	}

	payload := binary.BigEndian.AppendUint16(make([]byte, 0, betsCountSize), uint16(len(bets)))
	for _, b := range bets {
		serialized, err := serializeBet(payload, b)
		if err != nil {
			return err
		}
		payload = serialized
	}

	return SendMessage(socket, MessageTypeBets, payload)
}

func SendDone(socket io.Writer) error {
	return SendMessage(socket, MessageTypeDone, nil)
}

func ReceiveMessage(socket io.Reader) (MessageType, []byte, error) {
	header, err := safe_socket.RecvAll(socket, headerSize)
	if err != nil {
		return 0, nil, err
	}

	messageType := MessageType(header[0])
	length := binary.BigEndian.Uint32(header[typeSize:])
	if length > maxPayloadSize {
		return 0, nil, fmt.Errorf("payload too large: %d bytes", length)
	}

	payload, err := safe_socket.RecvAll(socket, int(length))
	if err != nil {
		return 0, nil, err
	}

	switch messageType {
	case MessageTypeAgency, MessageTypeBets, MessageTypeDone,
		MessageTypeAck, MessageTypeWinners, MessageTypeError:
		return messageType, payload, nil
	default:
		return 0, nil, fmt.Errorf("invalid message type: %d", messageType)
	}
}

func DeserializeErrorMessage(payload []byte) string {
	return string(payload)
}
