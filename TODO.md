define a formal delivery state machine (very useful later)?
Add cleanup task/trigger?

Persist a delivery cursor
Instead of inferring:
store next_offset durably per (topic, group)
advance it only after mark_inflight_batch succeeds
on restart, read it directly

tracing (no more prints..)

auth

clusters

stats

admin/dashboard

cli

protocol

CLIENT

split to workspace(multi crate)