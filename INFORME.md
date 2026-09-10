# Informe del TP Nivelador

## Descripción general

El sistema representa un sistema cliente-servidor que representa varias agencias de lotería (clientes) comunicándose con una Lotería central (servidor).

Cada cliente lee las apuestas de su archivo de entrada, se identifica con un número de agencia y las envía agrupadas en batches. El servidor las almacena, realiza el sorteo, y envía a cada cliente los ganadores de las apuestas que enviaron respectivamente.

## Protocolo de comunicación

Se implementó un protocolo binario. La conversión entre enteros y bytes se realiza con orden big-endian en ambos lenguajes para que interpretan los mensajes de la misma forma, sin depender del tamaño nativo de los tipos de cada uno.

### Framing

Cada mensaje consta de un encabezado de 5 bytes y un payload de longitud variable:

| Campo | Tamaño | Descripción |
|---|---:|---|
| Tipo | 1 byte | Clase de mensaje |
| Longitud | 4 bytes | Cantidad de bytes que ocupa el payload, en big-endian |
| Payload | Variable | Contenido del mensaje |

Esto permite que el receptor sepa hasta qué parte del flujo de bytes recibido (porque es TCP) debe leer para obtener el mensaje completo.

### Tipos de mensaje

| Código | Nombre | Dirección | Payload |
|---:|---|---|---|
| `0x01` | `AGENCY` | Cliente → servidor | Identificador de la agencia |
| `0x02` | `BETS` | Cliente → servidor | Un lote de apuestas |
| `0x03` | `DONE` | Cliente → servidor | Vacío. La agencia terminó de enviar |
| `0x04` | `ACK` | Servidor → cliente | Vacío. Confirma un lote completo procesado |
| `0x05` | `WINNERS` | Servidor → cliente | Apuestas ganadoras de la agencia |
| `0x06` | `ERROR` | Servidor → cliente | Descripción del error |



## Flujo de comunicación

1. El cliente abre la conexión y envía `AGENCY`.
2. Recorre `INPUT_FILE`, agrupa apuestas hasta completar `BATCH_SIZE` y envía cada lote como `BETS`.
3. Espera el `ACK` antes de continuar con el siguiente lote. Si al terminar el archivo queda un lote incompleto, también lo envía.
4. Envía `DONE` y espera `WINNERS` o `ERROR`. La respuesta puede demorarse, ya que el servidor la retiene hasta alcanzar el quórum de agencias.
5. Escribe los ganadores en `OUTPUT_FILE` y libera sus recursos.



## Concurrencia

El servidor atiende a las agencias con un thread handler por conexión. El thread principal ejecuta únicamente el ciclo de aceptación: crea el thread, lo registra y vuelve a aceptar. Así puede atender a los clientes mientras acepta otras conexiones.

### Recursos compartidos

| Recurso | Acceso | Protección |
|---|---|---|
| Archivo `bets.csv` en `Lottery` | `store_bets` escribe, `load_bets` lee | `threading.Lock` |
| Contador de agencias finalizadas | Todos los threads | `threading.Condition` |


### Barrera de quórum

El mínimo se configura con `AGENCY_QUORUM_MIN`; si no está definida se usa 1. La sincronización se resuelve con una `threading.Condition` que funciona como barrera: el sorteo no se hace hasta que no se cumpla el quorum, y ninguna agencia recibe sus ganadores hasta entonces.


## Finalización ordenada

Ambos procesos manejan `SIGTERM` y terminan liberando sus recursos en un tiempo acotado. En ningún caso se fuerza la salida: la señal se traduce en un flujo de error normal que se propaga hasta el punto de entrada, y solo allí se devuelve el código de retorno.
