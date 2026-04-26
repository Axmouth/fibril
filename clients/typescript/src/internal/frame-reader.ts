import type { Socket } from "node:net";
import { tryDecodeFrame, type Frame } from "../codec.js";

/**
 * Wraps a `net.Socket` and emits parsed frames. Handles arbitrary chunking
 * of bytes from the underlying TCP stream.
 *
 * Use as an async iterable:
 *
 *   for await (const frame of new FrameReader(socket)) { ... }
 *
 * The iterator ends cleanly when the socket closes. Socket errors are
 * surfaced as iterator errors.
 */
export class FrameReader implements AsyncIterable<Frame> {
  #socket: Socket;
  #buffer: Uint8Array = new Uint8Array(0);
  #pendingResolve: ((value: IteratorResult<Frame>) => void) | null = null;
  #pendingReject: ((reason: unknown) => void) | null = null;
  #queue: Frame[] = [];
  #closed = false;
  #error: Error | null = null;

  constructor(socket: Socket) {
    this.#socket = socket;
    socket.on("data", this.#onData);
    socket.on("end", this.#onEnd);
    socket.on("close", this.#onEnd);
    socket.on("error", this.#onError);
  }

  #onData = (chunk: Buffer): void => {
    // Append chunk; accumulator strategy is naive concat. For high-throughput
    // workloads we'd want a chunk list + scatter parsing, but typical client
    // workloads don't warrant the complexity.
    if (this.#buffer.byteLength === 0) {
      this.#buffer = new Uint8Array(
        chunk.buffer,
        chunk.byteOffset,
        chunk.byteLength,
      );
    } else {
      const merged = new Uint8Array(this.#buffer.byteLength + chunk.byteLength);
      merged.set(this.#buffer, 0);
      merged.set(chunk, this.#buffer.byteLength);
      this.#buffer = merged;
    }

    while (true) {
      const result = tryDecodeFrame(this.#buffer);
      if (!result) break;
      this.#buffer = this.#buffer.subarray(result.consumed);
      this.#deliver(result.frame);
    }

    // Compact the buffer if we've consumed a lot, to avoid holding a long
    // tail. (subarray shares the underlying ArrayBuffer.)
    if (this.#buffer.byteLength === 0) {
      this.#buffer = new Uint8Array(0);
    }
  };

  #onEnd = (): void => {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#pendingResolve) {
      const resolve = this.#pendingResolve;
      this.#pendingResolve = null;
      this.#pendingReject = null;
      resolve({ value: undefined, done: true });
    }
  };

  #onError = (err: Error): void => {
    this.#error = err;
    this.#closed = true;
    if (this.#pendingReject) {
      const reject = this.#pendingReject;
      this.#pendingResolve = null;
      this.#pendingReject = null;
      reject(err);
    }
  };

  #deliver(frame: Frame): void {
    if (this.#pendingResolve) {
      const resolve = this.#pendingResolve;
      this.#pendingResolve = null;
      this.#pendingReject = null;
      resolve({ value: frame, done: false });
    } else {
      this.#queue.push(frame);
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<Frame> {
    return {
      next: (): Promise<IteratorResult<Frame>> => {
        if (this.#queue.length > 0) {
          const frame = this.#queue.shift()!;
          return Promise.resolve({ value: frame, done: false });
        }
        if (this.#error) {
          const err = this.#error;
          this.#error = null;
          return Promise.reject(err);
        }
        if (this.#closed) {
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise<IteratorResult<Frame>>((resolve, reject) => {
          this.#pendingResolve = resolve;
          this.#pendingReject = reject;
        });
      },
      return: (): Promise<IteratorResult<Frame>> => {
        this.#detach();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  #detach(): void {
    this.#socket.off("data", this.#onData);
    this.#socket.off("end", this.#onEnd);
    this.#socket.off("close", this.#onEnd);
    this.#socket.off("error", this.#onError);
  }
}
