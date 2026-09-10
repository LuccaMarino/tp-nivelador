import socket
import logger
import protocol

from lottery import Lottery


class Server:
    def __init__(self, server_host: str, server_port: int, storage_path: str) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path)
        
    
    def _receive_agency_id(self, client_socket) -> int:
        message = protocol.receive_message(client_socket)
        if message is None:
            raise ConnectionError("connection closed by client before sending agency id")
        
        message_type, payload = message
        if message_type != protocol.MessageType.AGENCY:
            raise protocol.ProtocolError(f"expected agency id message, got {message_type.name}")
        
        return protocol.deserialize_agency_id(payload)
    
    
    def _receive_bets(self, client_socket, agency_id) -> int:
        bets_count = 0
        while True:
            message = protocol.receive_message(client_socket)
            if message is None:
                raise ConnectionError("connection closed by client before the agency was done")
            
            message_type, payload = message
            if message_type == protocol.MessageType.DONE:
                return bets_count
            if message_type != protocol.MessageType.BETS:
                raise protocol.ProtocolError(f"expected a bets message, got {message_type.name}")
            
            bets = protocol.deserialize_bets(payload, agency_id)
            self.lottery.store_bets(bets)
            bets_count += len(bets)
            protocol.send_ack(client_socket)    # manda ACK luego de recibir y guardar las apuestas
    
    
    def _send_winners(self, client_socket, agency_id):
        action = "lottery-draw"
        logger.info(action, logger.LogResult.in_progress, "agency-id", agency_id)
        
        winners = []
        bets = self.lottery.load_bets()
        for bet in bets:
            if bet.agency_id == agency_id and self.lottery.has_won(bet):
                winners.append(bet)
        
        protocol.send_winners(client_socket, winners)
        logger.info(action, logger.LogResult.success, "agency-id", agency_id, "winners-count", len(winners))
    
    
    def _notify_error(self, client_socket, error_message):
        try:
            protocol.send_error(client_socket, error_message)
        except OSError:
            pass  # la conexion ya se cerró, no se puede mandar el mensaje de error

    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        logger.info(action, logger.LogResult.in_progress)
        try:
            agency_id = self._receive_agency_id(client_socket)
            bets_count = self._receive_bets(client_socket, agency_id)
            self._send_winners(client_socket, agency_id)
            logger.info(action, logger.LogResult.success, "agency-id", agency_id, "bets-count", bets_count)
        except protocol.ProtocolError as e:
            self._notify_error(client_socket, str(e))
            logger.error(action, logger.LogResult.fail, "agency-id", agency_id, "err", e)
        except OSError as e:
            logger.error(action, logger.LogResult.fail, "agency-id", agency_id, "err", e)

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail, "err", e)
                    raise e
                logger.info(action, logger.LogResult.success)
                with client_socket:
                    self._handle_client(client_socket)
