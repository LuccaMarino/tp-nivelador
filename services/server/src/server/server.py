import socket
import logger
import protocol
import threading

from lottery import Lottery


class Server:
    def __init__(self, server_host: str, server_port: int, storage_path: str, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path)
        self.agency_quorum_min = agency_quorum_min
        self._storage_lock = threading.Lock()           # para proteger el storage
        self._quorum_condition = threading.Condition()  # para proteger _agencies_done y despertar los hilos
        self._agencies_done = 0
        self._client_threads = []
    
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
            with self._storage_lock:    # protego storage
                self.lottery.store_bets(bets)
            bets_count += len(bets)
            protocol.send_ack(client_socket)    # manda ACK luego de recibir y guardar las apuestas
    
    def _await_agency_quorum(self, agency_id):
        action = "await-agency-quorum"
        with self._quorum_condition:    # protego _agencies_done
            self._agencies_done += 1
            logger.info(action, logger.LogResult.in_progress, "agency-id", agency_id,
                        "agencies-done", self._agencies_done, 
                        "agency-quorum-min", self.agency_quorum_min)
            self._quorum_condition.notify_all()
            while self._agencies_done < self.agency_quorum_min:
                self._quorum_condition.wait()   # espero a que se cumpla el quorum
        logger.info(action, logger.LogResult.success, "agency-id", agency_id)


    def _send_winners(self, client_socket, agency_id):
        action = "lottery-draw"
        logger.info(action, logger.LogResult.in_progress, "agency-id", agency_id)
        
        winners = []
        with self._storage_lock:    # protego storage, aunque es solo lectura
            bets = self.lottery.load_bets()
            for bet in bets:
                if bet.agency_id == agency_id and self.lottery.has_won(bet):
                    winners.append(bet)
        
        protocol.send_winners(client_socket, winners)
        logger.info(action, logger.LogResult.success, "agency-id", agency_id, 
                    "winners-count", len(winners))
    
    
    def _notify_error(self, client_socket, error_message):
        try:
            protocol.send_error(client_socket, error_message)
        except OSError:
            pass  # la conexion ya se cerró, no se puede mandar el mensaje de error

    def _handle_client(self, client_socket):
        action = "handle-client"
        agency_id = None
        logger.info(action, logger.LogResult.in_progress)
        with client_socket:
            try:
                agency_id = self._receive_agency_id(client_socket)
                bets_count = self._receive_bets(client_socket, agency_id)
                self._await_agency_quorum(agency_id)    # ya tengo las apuestas, espero al quorum para el sorteo
                self._send_winners(client_socket, agency_id)
                logger.info(action, logger.LogResult.success, "agency-id", agency_id, 
                            "bets-count", bets_count)
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
                client_handler = threading.Thread(target=self._handle_client, args=(client_socket,))
                self._client_threads.append(client_handler)
                client_handler.start()
