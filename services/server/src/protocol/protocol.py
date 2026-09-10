from enum import IntEnum

from lottery import Bet
import safe_socket

_BYTE_ORDER = "big"
_TYPE_SIZE = 1
_LENGTH_SIZE = 4
_HEADER_SIZE = _TYPE_SIZE + _LENGTH_SIZE
_STRING_LENGTH_SIZE = 1
_MAX_STRING_LENGTH = 2 ** (8 * _STRING_LENGTH_SIZE) - 1  # 255 bytes de largo max
_AGENCY_ID_SIZE = 2
_DOCUMENT_SIZE = 4 
_NUMBER_SIZE = 4
_BIRTHDATE_SIZE = 10
_WINNERS_COUNT_SIZE = 2
_BETS_COUNT_SIZE = 2
_MAX_PAYLOAD_SIZE = 1024 * 1024  # 1 MB (capaz es mucho??) 

class MessageType(IntEnum):
    AGENCY = 0x01
    BETS = 0x02
    DONE = 0x03
    ACK = 0x04
    WINNERS = 0x05
    ERROR = 0x06
    
class ProtocolError(Exception):
    pass


def _serialize_uint(value, size):
    if value < 0 or value >= 2 ** (8 * size):
        raise ProtocolError("uint out of range")
    return value.to_bytes(size, _BYTE_ORDER)


def _deserialize_uint(data, offset, size):
    end = offset + size
    if end > len(data):
        raise ProtocolError("payload too short to decode uint")
    return int.from_bytes(data[offset:end], _BYTE_ORDER), end


def _serialize_string(value):
    encoded = value.encode("utf-8")
    if len(encoded) > _MAX_STRING_LENGTH:
        raise ProtocolError(f"string too long to serialize: {len(encoded)} bytes")
    return _serialize_uint(len(encoded), _STRING_LENGTH_SIZE) + encoded


def _deserialize_string(data, offset):
    length, offset = _deserialize_uint(data, offset, _STRING_LENGTH_SIZE)
    end = offset + length
    if end > len(data):
        raise ProtocolError("payload too short to decode string")
    return data[offset:end].decode("utf-8"), end

# Para birthdate, que tiene un largo fijo (10 bytes)
def _serialize_fixed_string(value, size):
    encoded = value.encode("utf-8")
    if len(encoded) != size:
        raise ProtocolError(f"unexpected fixed string size: {len(encoded)} bytes")
    return encoded


def _deserialize_fixed_string(data, offset, size):
    end = offset + size
    if end > len(data):
        raise ProtocolError("payload too short to decode fixed string")
    return data[offset:end].decode("utf-8"), end


def _serialize_bet(bet: Bet):
    return (
        _serialize_string(bet.first_name)
        + _serialize_string(bet.last_name)
        + _serialize_uint(bet.document, _DOCUMENT_SIZE)
        + _serialize_fixed_string(bet.birthdate, _BIRTHDATE_SIZE)
        + _serialize_uint(bet.number, _NUMBER_SIZE)
    )


def _deserialize_bet(data, offset, agency_id):
    first_name, offset = _deserialize_string(data, offset)
    last_name, offset = _deserialize_string(data, offset)
    document, offset = _deserialize_uint(data, offset, _DOCUMENT_SIZE)
    birthdate, offset = _deserialize_fixed_string(data, offset, _BIRTHDATE_SIZE)
    number, offset = _deserialize_uint(data, offset, _NUMBER_SIZE)
    return Bet(agency_id, first_name, last_name, document, birthdate, number), offset


def deserialize_agency_id(payload):
    agency_id, offset = _deserialize_uint(payload, 0, _AGENCY_ID_SIZE)
    if offset != len(payload):
        raise ProtocolError("unexpected payload length for agency id")
    return agency_id


def deserialize_bets(payload, agency_id):
    bets_count, offset = _deserialize_uint(payload, 0, _BETS_COUNT_SIZE)
    bets = []
    
    for _ in range(bets_count):
        bet, offset = _deserialize_bet(payload, offset, agency_id)
        bets.append(bet)
    if offset != len(payload):
        raise ProtocolError("unexpected payload length for bets")
    return bets


def send_message(socket, message_type, payload=b""):
    if len(payload) > _MAX_PAYLOAD_SIZE:
        raise ProtocolError("payload too large")
    header = _serialize_uint(message_type, _TYPE_SIZE) + _serialize_uint(len(payload), _LENGTH_SIZE)
    safe_socket.send_all(socket, header + payload)
    

def send_ack(socket):
    send_message(socket, MessageType.ACK)
    
    
def send_error(socket, error_message=""):
    send_message(socket, MessageType.ERROR, error_message.encode("utf-8"))
    

def send_winners(socket, winners):
    payload = _serialize_uint(len(winners), _WINNERS_COUNT_SIZE)
    for winner in winners:
        payload += _serialize_bet(winner)
    send_message(socket, MessageType.WINNERS, payload)


def receive_message(socket):
    header = safe_socket.recv_all(socket, _HEADER_SIZE)
    if not header:
        return None  # la conexion se cerró
    
    message_type, offset = _deserialize_uint(header, 0, _TYPE_SIZE)
    length, _ = _deserialize_uint(header, offset, _LENGTH_SIZE)
    if length > _MAX_PAYLOAD_SIZE:
        raise ProtocolError("payload too large")
    payload = safe_socket.recv_all(socket, length)
    try:
        return MessageType(message_type), payload
    except ValueError:
        raise ProtocolError(f"invalid message type: {message_type}")
    
