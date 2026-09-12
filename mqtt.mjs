const textEncoder = new TextEncoder();
const strictTextDecoder = new TextDecoder('utf-8', { fatal: true });

function concatBytes(...parts) {
  const size = parts.reduce((sum, part) => sum + part.length, 0);
  const result = new Uint8Array(size);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

function encodeUint16(value) {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
    throw new RangeError(`MQTT uint16 out of range: ${value}`);
  }
  return Uint8Array.from([value >> 8, value & 0xff]);
}

function encodeUtf8(value) {
  const bytes = textEncoder.encode(value);
  return concatBytes(encodeUint16(bytes.length), bytes);
}

export function encodeRemainingLength(value) {
  if (!Number.isInteger(value) || value < 0 || value > 268435455) {
    throw new RangeError(`MQTT remaining length out of range: ${value}`);
  }
  const output = [];
  do {
    let digit = value % 128;
    value = Math.floor(value / 128);
    if (value > 0) digit |= 0x80;
    output.push(digit);
  } while (value > 0);
  return Uint8Array.from(output);
}

function encodePacket(typeAndFlags, body) {
  return concatBytes(
    Uint8Array.from([typeAndFlags]),
    encodeRemainingLength(body.length),
    body,
  );
}

export function encodeConnect(clientId, keepAliveSeconds = 30) {
  const body = concatBytes(
    encodeUtf8('MQTT'),
    Uint8Array.from([4, 0x02]),
    encodeUint16(keepAliveSeconds),
    encodeUtf8(clientId),
  );
  return encodePacket(0x10, body);
}

export function encodeSubscribe(packetId, filters) {
  if (!filters.length) throw new RangeError('MQTT SUBSCRIBE needs a filter');
  const bodyParts = [encodeUint16(packetId)];
  for (const item of filters) {
    const filter = typeof item === 'string' ? item : item.filter;
    const qos = typeof item === 'string' ? 0 : item.qos;
    if (![0, 1, 2].includes(qos)) throw new RangeError(`Invalid requested QoS: ${qos}`);
    bodyParts.push(encodeUtf8(filter), Uint8Array.from([qos]));
  }
  return encodePacket(0x82, concatBytes(...bodyParts));
}

export function encodeAck(packetType, packetId) {
  const flags = packetType === 6 ? 0x02 : 0;
  return encodePacket((packetType << 4) | flags, encodeUint16(packetId));
}

export function encodePingReq() {
  return Uint8Array.from([0xc0, 0x00]);
}

export function encodeDisconnect() {
  return Uint8Array.from([0xe0, 0x00]);
}

export function copyBytes(value) {
  if (value instanceof Uint8Array) return value.slice();
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
  }
  throw new TypeError('Expected ArrayBuffer or typed array');
}

export class MqttDecoder {
  #buffer = new Uint8Array(0);

  push(chunk) {
    const incoming = copyBytes(chunk);
    this.#buffer = concatBytes(this.#buffer, incoming);
    const packets = [];
    let offset = 0;

    while (offset + 2 <= this.#buffer.length) {
      const first = this.#buffer[offset];
      let multiplier = 1;
      let remainingLength = 0;
      let lengthBytes = 0;
      let completeLength = false;
      while (lengthBytes < 4) {
        if (offset + 1 + lengthBytes >= this.#buffer.length) break;
        const digit = this.#buffer[offset + 1 + lengthBytes];
        remainingLength += (digit & 0x7f) * multiplier;
        lengthBytes += 1;
        if ((digit & 0x80) === 0) {
          completeLength = true;
          break;
        }
        multiplier *= 128;
      }
      if (!completeLength) break;
      if (lengthBytes > 4) throw new Error('Malformed MQTT remaining length');
      const totalLength = 1 + lengthBytes + remainingLength;
      if (offset + totalLength > this.#buffer.length) break;
      const rawBytes = this.#buffer.slice(offset, offset + totalLength);
      const bodyStart = offset + 1 + lengthBytes;
      packets.push({
        type: first >> 4,
        flags: first & 0x0f,
        body: this.#buffer.slice(bodyStart, bodyStart + remainingLength),
        rawBytes,
      });
      offset += totalLength;
    }

    this.#buffer = this.#buffer.slice(offset);
    return packets;
  }
}

export function parsePublish(packet) {
  const { flags, body } = packet;
  const qos = (flags >> 1) & 0x03;
  if (qos === 3 || body.length < 2) throw new Error('Malformed MQTT PUBLISH');
  const topicLength = (body[0] << 8) | body[1];
  let offset = 2;
  if (offset + topicLength > body.length) throw new Error('Malformed MQTT topic');
  const topic = strictTextDecoder.decode(body.slice(offset, offset + topicLength));
  offset += topicLength;
  let packetId = null;
  if (qos > 0) {
    if (offset + 2 > body.length) throw new Error('Malformed MQTT packet identifier');
    packetId = (body[offset] << 8) | body[offset + 1];
    offset += 2;
  }
  return {
    topic,
    qos,
    dup: Boolean(flags & 0x08),
    retain: Boolean(flags & 0x01),
    packetId,
    payloadBytes: body.slice(offset),
  };
}

export function decodePayloadBytes(bytes) {
  const exact = copyBytes(bytes);
  try {
    return {
      payloadRaw: strictTextDecoder.decode(exact),
      payloadBytesBase64: null,
      encoding: 'utf8',
    };
  } catch {
    return {
      payloadRaw: null,
      payloadBytesBase64: Buffer.from(exact).toString('base64'),
      encoding: 'base64',
    };
  }
}

export function parseConnAck(body) {
  if (body.length !== 2) throw new Error('Malformed MQTT CONNACK');
  return { sessionPresent: Boolean(body[0] & 0x01), returnCode: body[1] };
}

export function parseSubAck(body) {
  if (body.length < 3) throw new Error('Malformed MQTT SUBACK');
  const packetId = (body[0] << 8) | body[1];
  return { packetId, grantedQos: [...body.slice(2)] };
}

export function parsePacketId(body) {
  if (body.length < 2) throw new Error('Malformed MQTT acknowledgement');
  return (body[0] << 8) | body[1];
}

export function bytesToBase64(bytes) {
  return Buffer.from(copyBytes(bytes)).toString('base64');
}
