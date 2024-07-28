const net = require("net");
const fs = require("fs");

const HOST = "localhost";
const PORT = 3000;

const PACKET_SIZE = 17;

let packets = [];
let receivedSequences = new Set();
let clientEnded = false;

function createRequestPayload(callType, resendSeq = 0) {
  const buffer = Buffer.alloc(2);
  buffer.writeUInt8(callType, 0);
  buffer.writeUInt8(resendSeq, 1);
  return buffer;
}

function parsePacket(buffer) {
  let offset = 0;
  const symbol = buffer.toString("ascii", offset, offset + 4);
  offset += 4;
  const buysellindicator = buffer.toString("ascii", offset, offset + 1);
  offset += 1;
  const quantity = buffer.readInt32BE(offset);
  offset += 4;
  const price = buffer.readInt32BE(offset);
  offset += 4;
  const packetSequence = buffer.readInt32BE(offset);
  return { symbol, buysellindicator, quantity, price, packetSequence };
}

function handleData(buffer) {
  let offset = 0;
  while (offset < buffer.length) {
    const packet = parsePacket(buffer.slice(offset, offset + PACKET_SIZE));
    packets.push(packet);
    receivedSequences.add(packet.packetSequence);
    offset += PACKET_SIZE;
  }
}

function requestMissingPackets(client, maxSequence) {
  for (let i = 1; i <= maxSequence; i++) {
    if (!receivedSequences.has(i) && !clientEnded) {
      client.write(createRequestPayload(2, i));
    }
  }
}

const client = net.createConnection({ host: HOST, port: PORT }, () => {
  console.log("Connected to BetaCrew exchange server");
  client.write(createRequestPayload(1));
});

client.on("data", (data) => {
  handleData(data);
});

client.on("end", () => {
  console.log("Disconnected from server. Processing data...");
  clientEnded = true;

  const maxSequence = Math.max(...Array.from(receivedSequences));

  requestMissingPackets(client, maxSequence);

  setTimeout(() => {
    const outputFile = "output.json";
    fs.writeFileSync(outputFile, JSON.stringify(packets, null, 2));
    console.log(`Data written to ${outputFile}`);
    client.end();
  }, 2000);
});

client.on("error", (err) => {
  console.error(`Error: ${err.message}`);
  clientEnded = true;
});
