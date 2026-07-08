import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:net";
import { readFileSync } from "node:fs";
import { ClientOptions } from "../src/index.js";
import { deserializeByContentType } from "../src/message.js";

// Shared source of truth for client-local guide wording (see
// clients/error_guides.json), the error-message analogue of wire_vectors.json.
const guides = JSON.parse(
  readFileSync(new URL("../../error_guides.json", import.meta.url), "utf8"),
) as Record<string, { must_contain: string[] }>;

async function closedPort(): Promise<number> {
  return new Promise((resolve) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
  });
}

test("connection_refused carries the shared guide", async () => {
  const port = await closedPort();
  let message = "";
  try {
    await new ClientOptions().disableAutoReconnect().connect({ host: "127.0.0.1", port });
    assert.fail("connecting to a closed port must throw");
  } catch (err) {
    message = String((err as Error).message).toLowerCase();
  }
  for (const keyword of guides.connection_refused.must_contain) {
    assert.ok(
      message.includes(keyword.toLowerCase()),
      `connection_refused guide is missing ${keyword}: ${message}`,
    );
  }
});

test("decode_malformed_body carries the shared guide", () => {
  let message = "";
  try {
    deserializeByContentType("application/json", new TextEncoder().encode("not json"));
    assert.fail("decoding a malformed json body must throw");
  } catch (err) {
    message = String((err as Error).message).toLowerCase();
  }
  for (const keyword of guides.decode_malformed_body.must_contain) {
    assert.ok(
      message.includes(keyword.toLowerCase()),
      `decode_malformed_body guide is missing ${keyword}: ${message}`,
    );
  }
});
