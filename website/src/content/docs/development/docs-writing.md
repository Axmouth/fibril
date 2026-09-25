---
title: Documentation style
description: How Fibril docs should separate user-facing behavior from implementation detail.
---

Fibril documentation should start from the user's problem and explain the behavior that addresses it.

Most user-facing pages should follow this shape:

1. What problem this addresses.
2. What Fibril does.
3. What conditions activate or block the behavior.
4. What tradeoffs or limits the user should expect.
5. Links to deeper implementation notes only when useful.

Avoid making regular concept pages depend on internal names such as queue actors, cached handles, or storage materialization unless the name is also part of the user-facing model.

Development notes explain implementation mechanisms. User-facing pages should link to them as optional deeper reading.

Use the docs sections deliberately:

- `concepts`, `reliability`, `configuration`, `quickstart`, `clients`, and
  `admin-dashboard` should explain what users can do and what behavior they can
  rely on.
- `status` should be a concise user-facing feature matrix.
- `implemented-surface` can be more detailed, but should still say which client
  or operator path is wired before naming internals.
- `development` is the right place for implementation mechanisms, tradeoff
  records, and future design policies.

## Keeping documentation current

The [roadmap](/roadmap/) and active planning documents contain remaining work,
priorities, dependencies and acceptance criteria. Completed capabilities belong
in [implemented surface](/implemented-surface/), with the interfaces that expose
them and their operating conditions. [Project status](/status/) summarizes
maturity. The changelog records change history.

When a capability lands, update these together:

1. Add or update its implemented-surface entry, including client coverage,
   configuration, limits and links to the relevant guide.
2. Update the feature matrix and user guide for any changed public behavior.
3. Remove completed work from the roadmap and active plans. Keep any remaining
   gap as a specific task with acceptance criteria.
4. Preserve useful design rationale and test evidence in development notes or
   an archived design record, linked from the active work where relevant.
5. Record the change in the changelog. Do not copy the completion history back
   into the roadmap.

Write current behavior in the present tense and pending work as concrete tasks.
Use direct statements about behavior, requirements and limits. Avoid rhetorical
contrasts such as “not X, but Y” and slogans about milestones.
Avoid semicolons in prose, including captions and interface text. Use a full stop
or rephrase the sentence. This rule does not apply to code syntax.
Each page should stand on its own without answering earlier wording or referring
to a conversation. Release labels require verification that the release was
published. Version numbers, tags and documentation snapshots can exist before
publication. Edit current unversioned docs when behavior changes. Versioned
snapshots retain their historical contents.

## Short engineering records

Add significant optimizations and bugs to
[optimization and bug notes](/development/engineering-notes/). Aim for two or
three sentences per entry: the mechanism or trigger, the measured result or
correctness effect, and a commit or detailed report link. Label unresolved
findings and experiments that were not adopted. Include workload conditions
when quoting measurements, and keep detailed traces and benchmark tables in
the linked records.


## Animated architecture stories

`BrokerStory.astro` renders the shared server enclosures, controls and transcript.
`broker-story/scenarios.mjs` owns the stage descriptions, assignments, message
paths and illustrative persistence windows. `player.mjs` supplies playback and
frame export. Embed `<BrokerStory scene="failover" />`, `scene="delivery"` or
`scene="placement"` or `scene="checkpoint"` in the relevant MDX page. The branding build copies canonical
mascot frames from the real dashboard. New sprite variants belong in that shared
artwork source.

Scene time explains ordering and overlap. Label it as illustrative, keep required
confirmation/recovery barriers in the model, and identify current, experimental
and proposed paths. The scene tests check those narrative boundaries. They do
not prove broker correctness or benchmark latency. Next/Back and the position
slider support close reading. Reduced motion disables autoplay and mascot motion.
The transcript remains available without JavaScript. Save frame pauses playback
and exports the current SVG with embedded artwork and explanatory text.

The scene model uses checked JavaScript with contracts in `types.d.ts`. Run
`npm run story:test` in `website` for strict type checks and narrative invariants.
The normal website build runs both. Add new scene IDs, endpoints and frame data
to the shared types before extending the model. The browser receives ordinary
JavaScript with no type-checking runtime.

The site favicon is `crates/admin/admin-ui/img/fibril-mark.svg`. The branding
build copies that source and produces 16, 32 and 48 pixel PNGs with Sharp, already
used by Astro and declared directly for this build step. Both the landing page
and documentation reference these generated assets. The live dashboard retains
its separate status faces.
