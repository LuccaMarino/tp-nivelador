package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	for len(bytes) > 0 {
		n, err := socket.Write(bytes)
		if err != nil {
			return err
		}
		bytes = bytes[n:] // muevo el slice en n posiciones para enviar los bytes restantes
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	total := 0
	for total < size {
		n, err := socket.Read(buff[total:])
		total += n
		if total == size { // si ya se leyeron todos los bytes esperados salgo del loop
			break
		}
		// manejo el err despues de procesar los bytes leidos,
		// lo recomienda la documentacion de io.Reader
		if err != nil {
			return nil, err
		}
	}
	return buff, nil
}
