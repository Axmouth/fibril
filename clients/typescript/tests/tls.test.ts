import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer as createPlainServer, type Socket } from "node:net";
import {
  createServer as createTlsServer,
  type Server as TlsServer,
  type TLSSocket,
} from "node:tls";
import { X509Certificate } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildFrame, tryDecodeFrame, encodeFrame, type Frame } from "../src/codec.js";
import { COMPLIANCE_STRING, Op, PROTOCOL_V1 } from "../src/protocol.js";
import { Client, ClientOptions } from "../src/client.js";
import {
  ERR_TLS_REQUIRED,
  TlsCertificateUntrustedError,
  TlsNotSupportedByBrokerError,
  TlsRequiredByBrokerError,
} from "../src/errors.js";

const uuid = (fill: number): Uint8Array => new Uint8Array(16).fill(fill);

function helloOk(): Record<string, unknown> {
  return {
    protocol_version: PROTOCOL_V1,
    owner_id: uuid(0x10),
    client_id: uuid(0x00),
    resume_token: uuid(0x20),
    resume_outcome: "new",
    server_name: "fake-tls",
    compliance: COMPLIANCE_STRING,
  };
}

/**
 * Mint a throwaway CA and a localhost server certificate with openssl, so no
 * certificate material is ever committed to the repo.
 */
function mintCerts(): { dir: string; caPem: string; serverPem: string; serverKey: string } {
  const dir = mkdtempSync(join(tmpdir(), "fibril-ts-tls-"));
  const caKey = join(dir, "ca.key");
  const caPem = join(dir, "ca.pem");
  const serverKey = join(dir, "server.key");
  const serverCsr = join(dir, "server.csr");
  const serverPem = join(dir, "server.pem");
  const extFile = join(dir, "san.cnf");
  writeFileSync(extFile, "subjectAltName=DNS:localhost,IP:127.0.0.1\n");
  const ossl = (args: string[]) => execFileSync("openssl", args, { stdio: "pipe" });
  ossl([
    "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
    "-keyout", caKey, "-out", caPem, "-days", "2", "-nodes",
    "-subj", "/CN=Fibril TS Test CA",
  ]);
  ossl([
    "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
    "-keyout", serverKey, "-out", serverCsr, "-nodes", "-subj", "/CN=localhost",
  ]);
  ossl([
    "x509", "-req", "-in", serverCsr, "-CA", caPem, "-CAkey", caKey,
    "-CAcreateserial", "-out", serverPem, "-days", "2", "-extfile", extFile,
  ]);
  return { dir, caPem, serverPem, serverKey };
}

/** A fake broker speaking the wire over TLS, answering HELLO. */
class FakeTlsBroker {
  #server!: TlsServer;
  port = 0;

  async start(serverPem: string, serverKey: string, caPem: string): Promise<void> {
    // Serve leaf + CA like the real broker's generated mode, so a CA
    // fingerprint pin can match the presented chain.
    const chain = readFileSync(serverPem, "utf8") + readFileSync(caPem, "utf8");
    this.#server = createTlsServer(
      { cert: chain, key: readFileSync(serverKey) },
      (socket: TLSSocket) => {
        let buf = new Uint8Array(0);
        socket.on("data", (chunk) => {
          const merged = new Uint8Array(buf.byteLength + chunk.byteLength);
          merged.set(buf, 0);
          merged.set(chunk, buf.byteLength);
          buf = merged;
          while (true) {
            const r = tryDecodeFrame(buf);
            if (!r) break;
            buf = buf.subarray(r.consumed);
            if (r.frame.opcode === Op.Hello) {
              socket.write(encodeFrame(buildFrame(Op.HelloOk, r.frame.requestId, helloOk())));
            }
          }
        });
        socket.on("error", () => {});
      },
    );
    await new Promise<void>((resolve) => {
      this.#server.listen(0, "127.0.0.1", () => {
        const addr = this.#server.address();
        if (addr && typeof addr !== "string") this.port = addr.port;
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => this.#server.close(() => resolve()));
  }
}

test("TLS connect with caPath trust completes the handshake and HELLO", async () => {
  const { caPem, serverPem, serverKey } = mintCerts();
  const broker = new FakeTlsBroker();
  await broker.start(serverPem, serverKey, caPem);
  try {
    const client = await Client.connect(
      { host: "127.0.0.1", port: broker.port },
      new ClientOptions().withTlsCaPath(caPem).withTlsServerName("localhost"),
    );
    await client.shutdown();
  } finally {
    await broker.stop();
  }
});

test("TLS connect with a CA fingerprint pin matches the presented chain", async () => {
  const { caPem, serverPem, serverKey } = mintCerts();
  const broker = new FakeTlsBroker();
  await broker.start(serverPem, serverKey, caPem);
  try {
    const fingerprint = new X509Certificate(readFileSync(caPem)).fingerprint256;
    const client = await Client.connect(
      { host: "127.0.0.1", port: broker.port },
      new ClientOptions().withTlsCaFingerprint(fingerprint),
    );
    await client.shutdown();

    await assert.rejects(
      Client.connect(
        { host: "127.0.0.1", port: broker.port },
        new ClientOptions().withTlsCaFingerprint("00".repeat(32)),
      ),
      TlsCertificateUntrustedError,
    );
  } finally {
    await broker.stop();
  }
});

test("untrusted certificate fails as a trust error, not a transport error", async () => {
  const { caPem, serverPem, serverKey } = mintCerts();
  const broker = new FakeTlsBroker();
  await broker.start(serverPem, serverKey, caPem);
  try {
    await assert.rejects(
      Client.connect(
        { host: "127.0.0.1", port: broker.port },
        // OS roots cannot know the throwaway CA.
        new ClientOptions().withTls().withTlsServerName("localhost"),
      ),
      TlsCertificateUntrustedError,
    );
  } finally {
    await broker.stop();
  }
});

test("plaintext client maps the broker's 426 reply to TlsRequiredByBrokerError", async () => {
  const server = createPlainServer((socket: Socket) => {
    let buf = new Uint8Array(0);
    socket.on("data", (chunk) => {
      const merged = new Uint8Array(buf.byteLength + chunk.byteLength);
      merged.set(buf, 0);
      merged.set(chunk, buf.byteLength);
      buf = merged;
      const r = tryDecodeFrame(buf);
      if (r) {
        // What the real TLS listener does on sniffing a plaintext HELLO.
        socket.end(
          encodeFrame(
            buildFrame(Op.Error, r.frame.requestId, {
              code: ERR_TLS_REQUIRED,
              message: "this broker requires TLS",
            }),
          ),
        );
      }
    });
  });
  const port: number = await new Promise((resolve) =>
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      resolve(addr && typeof addr !== "string" ? addr.port : 0);
    }),
  );
  try {
    await assert.rejects(
      Client.connect({ host: "127.0.0.1", port }, new ClientOptions()),
      TlsRequiredByBrokerError,
    );
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

test("TLS client against a plaintext listener fails fast with the probable cause", async () => {
  // What the real plaintext listener does on sniffing a ClientHello: close.
  const server = createPlainServer((socket: Socket) => {
    socket.once("data", (chunk) => {
      if (chunk[0] === 0x16 && chunk[1] === 0x03) socket.destroy();
    });
  });
  const port: number = await new Promise((resolve) =>
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      resolve(addr && typeof addr !== "string" ? addr.port : 0);
    }),
  );
  try {
    await assert.rejects(
      Client.connect(
        { host: "127.0.0.1", port },
        new ClientOptions().withTlsCaFingerprint("11".repeat(32)),
      ),
      TlsNotSupportedByBrokerError,
    );
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
