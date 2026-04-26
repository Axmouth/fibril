/**
 * Bounded async queue with backpressure. Roughly equivalent to
 * `tokio::sync::mpsc` with a fixed buffer.
 *
 * - `send(value)` resolves once the value is in the buffer, blocking
 *   the producer when the buffer is full until space is available.
 * - `recv()` resolves with the next value, or `null` when the queue
 *   is closed and drained.
 * - Closing the queue causes pending `send()` calls to reject.
 * - The queue is also an `AsyncIterable<T>` for `for await` consumption.
 */
export class BoundedQueue<T> implements AsyncIterable<T> {
  #capacity: number;
  #buffer: T[] = [];
  #pendingSenders: Array<{ value: T; resolve: () => void; reject: (e: unknown) => void }> = [];
  #pendingReceiver: ((v: { value: T; done: false } | { value: undefined; done: true }) => void) | null = null;
  #closed = false;
  #closeError: Error | null = null;

  constructor(capacity: number) {
    if (capacity < 1) {
      throw new Error(`BoundedQueue capacity must be >= 1, got ${capacity}`);
    }
    this.#capacity = capacity;
  }

  /**
   * Send a value. Awaits until buffer space is available or queue is closed.
   * Rejects with the close error if the queue is closed before the send completes.
   */
  send(value: T): Promise<void> {
    if (this.#closed) {
      return Promise.reject(this.#closeError ?? new Error("queue closed"));
    }

    // Fast path: deliver directly to a waiting receiver.
    if (this.#pendingReceiver) {
      const resolve = this.#pendingReceiver;
      this.#pendingReceiver = null;
      resolve({ value, done: false });
      return Promise.resolve();
    }

    // Buffer has room.
    if (this.#buffer.length < this.#capacity) {
      this.#buffer.push(value);
      return Promise.resolve();
    }

    // Buffer is full; wait for space.
    return new Promise<void>((resolve, reject) => {
      this.#pendingSenders.push({ value, resolve, reject });
    });
  }

  /**
   * Receive the next value, or `null` if the queue is closed and drained.
   */
  recv(): Promise<T | null> {
    // Buffer has values: drain and wake a sender.
    if (this.#buffer.length > 0) {
      const value = this.#buffer.shift()!;
      this.#wakeOneSender();
      return Promise.resolve(value);
    }

    // Closed and empty: terminal.
    if (this.#closed) {
      return Promise.resolve(null);
    }

    // Wait for next send.
    return new Promise<T | null>((resolve) => {
      this.#pendingReceiver = (result) => {
        resolve(result.done ? null : result.value);
      };
    });
  }

  #wakeOneSender(): void {
    if (this.#pendingSenders.length === 0) return;
    if (this.#buffer.length >= this.#capacity) return;
    const next = this.#pendingSenders.shift()!;
    this.#buffer.push(next.value);
    next.resolve();
  }

  /**
   * Close the queue. Pending and future sends will reject; pending receivers
   * are resolved with `null` once the buffer drains. Buffered values remain
   * available to `recv()` until drained.
   */
  close(error?: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeError = error ?? null;

    // Reject pending senders.
    const err = error ?? new Error("queue closed");
    for (const s of this.#pendingSenders) {
      s.reject(err);
    }
    this.#pendingSenders = [];

    // If a receiver is waiting and there's nothing buffered, terminate it.
    if (this.#pendingReceiver && this.#buffer.length === 0) {
      const r = this.#pendingReceiver;
      this.#pendingReceiver = null;
      r({ value: undefined, done: true });
    }
  }

  /**
   * Synchronously remove and return all currently buffered values.
   * Useful during shutdown to handle abandoned items.
   */
  drain(): T[] {
    const out = this.#buffer;
    this.#buffer = [];
    return out;
  }

  get closed(): boolean {
    return this.#closed;
  }

  get size(): number {
    return this.#buffer.length;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    while (true) {
      const v = await this.recv();
      if (v === null) return;
      yield v;
    }
  }
}
