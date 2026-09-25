/** Shared educational scene contracts. Time values are illustrative units. */
export type NodeId = "a" | "b" | "c" | "d";
export type Endpoint = NodeId | "client";
export type Ink = "data" | "control" | "ack" | "delivery" | "failure";
export type SceneId = "failover" | "delivery" | "placement" | "checkpoint";
export interface Flow {
  from: Endpoint;
  to: Endpoint;
  label: string;
  kind: Ink;
}
export interface StepExtras {
  dead?: NodeId[];
  blocked?: boolean;
  checkpoint?: boolean;
  focus?: number;
  history?: { accepted: boolean; suffix: boolean; verified: boolean };
}
export interface Step extends StepExtras {
  title: string;
  text: string;
  roles: string[];
  flows: Flow[];
}
export interface Segment {
  lane: 0 | 1 | 2 | 3;
  start: number;
  end: number;
  label: string;
  kind: Ink;
}
export interface TimedFlow extends Flow {
  start: number;
  end: number;
}
export interface Variant<Id extends string = string> {
  id: Id;
  label: string;
}
export interface DeliveryVariant extends Variant<
  "durable" | "early" | "speculative" | "replicated-speculation"
> {
  badge: string;
  text: string;
  segments: Segment[];
  events: TimedFlow[];
  milestones: [time: number, title: string, text: string][];
}
export interface Scene<V extends Variant, L extends string> {
  title: string;
  badge: string;
  note: string;
  nodes: string[];
  layout: L;
  variants: V[];
}
export type FailoverScene = Scene<Variant<"recover" | "fenced">, "cluster">;
export type CheckpointScene = Scene<Variant<"agreement">, "cluster">;
export type PlacementScene = Scene<Variant<"spread">, "placement">;
export interface DeliveryScene extends Scene<DeliveryVariant, "delivery"> {
  lanes: string[];
}
export interface Assignment {
  name: string;
  owner: NodeId;
  followers: NodeId[];
  color: Ink;
}
export interface Frame extends StepExtras {
  title: string;
  text: string;
  roles: string[];
  badge: string;
  inactive?: NodeId[];
  index: number;
  count: number;
  duration: number;
  progress: number;
  steps: number[];
  flows?: Flow[];
  stepProgress?: number;
  events?: (TimedFlow & { progress: number; active: boolean })[];
  segments?: (Segment & { progress: number })[];
}
