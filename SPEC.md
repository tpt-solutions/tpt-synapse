# `.tptmq` Protocol Specification — v1

A lightweight, encrypted, binary telemetry protocol for battery- and data-constrained
IoT devices (ESP32-class hardware) reporting to a server over TCP or MQTT. Designed
for the TPT Fleet asset-tracking product but intentionally generic — no fleet-specific
concepts appear in the frame format itself.

Status: v1 draft. Not yet implemented in firmware or the ingestion service.

## Goals

- Minimize bytes on the wire (cellular data is billed per MB on rover SIMs).
- Authenticated encryption on every frame, including metadata-only frames — a
  BLE-sighting frame leaks a hub's own location plus which device it saw, which is
  the single most privacy-sensitive payload in the system, so there is no
  "unencrypted frame type" carve-out.
- Cheap enough to run on an ESP32 with hardware AES acceleration, without pulling in
  a crypto library beyond what ESP-IDF/mbedTLS already ships.
- Same encoder/decoder regardless of transport (TCP or MQTT) — only how the frame's
  bytes get from A to B changes.

## Frame layout

All multi-byte integers are little-endian. All frames share one layout:

```
Offset  Size  Field
0       1     magic        (0xA5)
1       1     version      (0x01)
2       1     frame_type   (see Frame types)
3       1     key_id       (which key epoch encrypted this frame, see Key rotation)
4       4     device_id    (uint32, plaintext — needed to look up the key before decrypt)
8       12    nonce        (GCM nonce, plaintext, MUST be unique per key — see Nonce policy)
20      2     payload_len  (uint16, length of ciphertext that follows, in bytes)
22      N     ciphertext   (payload_len bytes, AES-128-GCM output)
22+N    16    auth_tag     (GCM authentication tag)
38+N    2     crc16        (CRC-16/CCITT-FALSE over bytes [0, 38+N) )
```

Fixed overhead per frame: 22 (header) + 16 (tag) + 2 (crc16) = **40 bytes**, on top of
whatever the plaintext payload is (typically 6–20 bytes — see Frame types below).

The header is deliberately self-describing enough to frame a TCP byte stream without
an extra outer length prefix: a reader always reads the fixed 22-byte header first,
learns `payload_len` from it, then reads exactly `payload_len + 16 + 2` more bytes to
complete the frame.

The CRC16 is a cheap pre-filter: reject corrupt frames before spending CPU/battery on
an AES-GCM decrypt attempt. It is not a security mechanism — GCM's auth tag is what
actually authenticates the frame; the CRC only catches transport-level bit errors.

## Encryption

**AES-128-GCM**, one pre-shared 128-bit key per device (rover or hub), issued at
provisioning time. Chosen over ChaCha20-Poly1305 because ESP32 has hardware AES
acceleration (lower latency, lower power draw on a battery-powered tag), and
ESP-IDF's mbedTLS build already includes AES-GCM with no extra dependency.

- Key: 16 bytes, generated server-side per device.
- Nonce: 12 bytes (96 bits, the GCM-recommended size). Never reuse a nonce under the
  same key — see Nonce policy.
- AAD (additional authenticated data): the plaintext header bytes `[0, 20)`
  (magic, version, frame_type, key_id, device_id, nonce) are passed as GCM's AAD, so
  a tampered header (e.g. a swapped `device_id` or `frame_type`) fails authentication
  even though those fields aren't themselves encrypted.
- Tag: 16 bytes, standard GCM tag length.

### Nonce policy

GCM nonce reuse under the same key is catastrophic (breaks confidentiality for both
messages that shared it). Devices MUST construct nonces as:

```
nonce = [4B boot_salt (random, generated once per boot)] || [8B send_counter (monotonic, starts at persisted_counter)]
```

- `boot_salt`: 4 random bytes drawn from the ESP32 hardware RNG at boot. Makes nonces
  from different boot sessions collide only in the astronomically unlikely case of a
  boot_salt collision *and* an overlapping counter range.
- `send_counter`: an 8-byte counter incremented once per frame sent. To survive
  unclean reboots (power loss without a graceful NVS flush), devices persist the
  counter to NVS every ~100 frames and, on boot, resume from
  `persisted_counter + 1000` (a safety margin large enough to guarantee the counter
  never regresses relative to the last frame actually transmitted before a crash).

The server tracks a high-water-mark `(boot_salt, send_counter)` per device and
rejects any frame whose counter does not advance past the last accepted value for
the current `boot_salt` — this is the replay-protection mechanism, not a separate
frame field.

## Frame types

| Value | Name             | Direction       | Plaintext payload (before encryption) |
|-------|------------------|-----------------|----------------------------------------|
| 0x01  | POSITION_REPORT  | device → server | see below (17 bytes) |
| 0x02  | HEARTBEAT        | device → server | `timestamp`(4B), `battery_pct`(1B), `flags`(1B) — 6 bytes |
| 0x03  | BLE_SIGHTING     | hub → server    | see below (17 bytes) |
| 0x04  | ACK              | server → device | `acked_send_counter`(8B), `status`(1B: 0=ok, 1=duplicate, 2=error) — 9 bytes |
| 0x05  | REKEY            | server → device | `new_key`(16B), `new_key_id`(1B) — 17 bytes, sent encrypted under the *current* key |
| 0x06  | ERROR            | server → device | `code`(1B), `message`(variable, UTF-8, no null terminator) |

### 0x01 POSITION_REPORT

```
timestamp    4B  uint32, unix seconds
lat          4B  int32, degrees × 1e7
lng          4B  int32, degrees × 1e7
battery_pct  1B  uint8, 0–100
speed_kmh    1B  uint8
signal_rssi  1B  int8
source       1B  0 = gps, 1 = ble_rssi_estimate
flags        1B  reserved, send 0x00
```

### 0x03 BLE_SIGHTING

```
timestamp          4B  uint32, unix seconds
hub_lat            4B  int32, degrees × 1e7 (the reporting hub's own position)
hub_lng            4B  int32, degrees × 1e7
sighted_device_id  4B  uint32 (the rover that was seen via BLE advertisement)
rssi               1B  int8
```

Encrypting this frame (hub position + whose device it saw) is why encryption is
applied uniformly to all `.tptmq` traffic instead of being an opt-in per frame type.

## Transport bindings

Both bindings carry the exact same frame bytes — one encoder/decoder, two ways to
move it.

### TCP (primary)

A persistent TCP connection per device. Frames are written back-to-back; the 22-byte
fixed header gives the reader enough information (`payload_len`) to know how many
more bytes complete the frame, so no additional length-prefixing or delimiter is
needed at the transport layer. The server replies with an ACK frame (0x04) on the
same connection after a frame is decrypted, authenticated, and durably written.

### MQTT (fallback, Mosquitto)

Used only after the TCP path fails to connect or send after a small number of
retries (exact backoff/threshold is a firmware concern, not a protocol concern). The
entire `.tptmq` frame (all bytes described above) is published as an opaque binary
MQTT payload:

- `tptmq/{device_id}/up` — device → server, QoS 1
- `tptmq/{device_id}/down` — server → device (ACK/REKEY/ERROR frames), QoS 1

MQTT connections authenticate with per-device MQTT credentials, which are separate
from the `.tptmq` payload encryption key — losing/rotating one does not require
touching the other.

## Key provisioning

1. Server generates a random 128-bit key when a provisioning token is created
   (`fleet.provisioning_tokens` in the Fleet product, or the equivalent table in any
   other product embedding this protocol).
2. The key is transmitted to the device **only** over the physical serial connection
   during flashing (same step that writes the provisioning token), never over the
   air. This is the only point in the device's life where the key exists outside of
   NVS and the server's datastore.
3. Device stores the key in NVS. Using ESP-IDF's NVS encryption feature (flash
   encryption) is recommended so the key isn't recoverable from a dumped flash image.
4. Server stores the key encrypted at rest (not plaintext in the `tptmq_key` column)
   — application-level encryption or `pgcrypto`, keyed by a secret the ingestion
   service holds, not the database itself.

## Key rotation

`key_id` in every frame's header lets the server support two key epochs
simultaneously during a rotation window (old key still accepted for frames sent
before the device switches over).

Rotation flow:
1. Server sends a REKEY frame (0x05), encrypted and authenticated under the
   device's **current** key, containing the new key and its `key_id`.
2. Device writes the new key to NVS, starts using the new `key_id` on subsequent
   frames, and sends an ACK-like confirmation (implementation may reuse 0x04 with
   the new `key_id` in the header) so the server knows the switch happened.
3. Server retires the old `key_id` for that device after seeing a frame with the
   new one.

**Limitation (open risk, not solved by this spec):** REKEY is authenticated using
the *current* key. If a key is suspected compromised, an attacker holding that key
could also forge a REKEY confirmation or ignore/spoof the rotation. True recovery
from a suspected-compromised key requires physical re-provisioning (re-flash or a
serial re-write of a fresh key), the same as initial provisioning — this protocol
does not provide remote recovery from a fully compromised device key.

## Reference implementations

- `tptmq/js/` — Node.js reference encoder/decoder (`tptmq.js`), used by the droplet
  ingestion service.
- `tptmq/c/` — portable C reference encoder/decoder (`tptmq.h` / `tptmq.c`), targets
  ESP-IDF's mbedTLS AES-GCM API; also compiles under a plain desktop toolchain for
  testing.

Both implementations expose the same conceptual API: `tptmq_encode(frame_type, key,
key_id, device_id, nonce, payload) -> bytes` and `tptmq_decode(bytes, key_lookup_fn)
-> {frame_type, key_id, device_id, payload} | error`. Neither implementation includes
transport code (TCP/MQTT plumbing) or NVS/counter-persistence — those are
integration concerns for firmware and the ingestion service respectively.
