// Shared helpers for the runnable examples. Each example doubles as a light
// end-to-end test: it connects to a real broker, exercises one feature, and
// self-validates with assertions, exiting non-zero on failure so a runner (and
// CI) can treat the example suite as a smoke test. See run-all.sh.

import { ClientOptions, Client } from "@fibril/client";

/** Assert a condition, throwing a clear error if it does not hold. */
export function check(condition: boolean, message: string): void {
  if (!condition) throw new Error(`check failed: ${message}`);
}

/** Assert equality with a descriptive message. */
export function assertEq<T>(actual: T, expected: T, message: string): void {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${String(expected)}, got ${String(actual)}`);
  }
}

/** Connect to the broker named by FIBRIL_ADDR (default 127.0.0.1:9876). */
export function connect(clientName: string): Promise<Client> {
  const addr = process.env.FIBRIL_ADDR ?? "127.0.0.1:9876";
  const user = process.env.FIBRIL_USER ?? "fibril";
  const pass = process.env.FIBRIL_PASS ?? "fibril";
  return new ClientOptions({ clientName }).withAuth(user, pass).connect(addr);
}

/** A unique topic per run so repeated runs do not interfere. */
export function uniqueTopic(prefix: string): string {
  return `example.${prefix}.${Date.now()}.${Math.floor(Math.random() * 1e6)}`;
}

/**
 * Whether to run in bounded check mode (a short, self-validating burst) instead
 * of the default continuous run. Continuous examples watch packets flow until
 * Ctrl-C; check mode runs a fixed amount, asserts the outcome, and exits, which
 * is what run-all.sh and CI use. Bounded examples ignore this.
 */
export function checkMode(): boolean {
  return process.argv.includes("--check") || process.env.FIBRIL_CHECK === "1";
}

/** Resolve to `value` after `ms`, for racing against a blocking recv. */
export function timeout<T>(ms: number, value: T): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms));
}

/** Run an example body, print PASS/FAIL, and exit with the matching code. */
export async function runExample(name: string, body: () => Promise<void>): Promise<void> {
  try {
    await body();
    console.log(`PASS ${name}`);
    process.exit(0);
  } catch (err) {
    console.error(`FAIL ${name}: ${(err as Error).message}`);
    process.exit(1);
  }
}
