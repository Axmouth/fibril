import type { AdvertisedAddressMsg } from "../src/protocol.js";

/**
 * Build a single-element advertised-address list entry from a "host:port" string,
 * for tests. The port is the part after the last colon, so a bracketed IPv6 host
 * stays intact.
 */
export function adv(target: string): AdvertisedAddressMsg {
  const i = target.lastIndexOf(":");
  return { host: target.slice(0, i), port: Number(target.slice(i + 1)), tags: [] };
}
