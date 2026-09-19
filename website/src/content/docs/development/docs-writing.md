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
maturity; the changelog records change history.

When a capability lands, update these together:

1. Add or update its implemented-surface entry, including client coverage,
   configuration, limits and links to the relevant guide.
2. Update the feature matrix and user guide for any changed public behavior.
3. Remove completed work from the roadmap and active plans. Keep any remaining
   gap as a specific task with acceptance criteria.
4. Preserve useful design rationale and test evidence in development notes or
   an archived design record, linked from the active work where relevant.
5. Record the change in the changelog; do not copy the completion history back
   into the roadmap.

Write current behavior in the present tense and pending work as concrete tasks.
Use direct statements about behavior, requirements and limits. Avoid rhetorical
contrasts such as “not X, but Y” and slogans about milestones.
Each page should stand on its own without answering earlier wording or referring
to a conversation. Release labels require verification that the release was
published; version numbers, tags and documentation snapshots can exist before
publication. Edit current unversioned docs when behavior changes; versioned
snapshots retain their historical contents.

## Short engineering records

Add significant optimizations and bugs to
[optimization and bug notes](/development/engineering-notes/). Aim for two or
three sentences per entry: the mechanism or trigger, the measured result or
correctness effect, and a commit or detailed report link. Label unresolved
findings and experiments that were not adopted. Include workload conditions
when quoting measurements, and keep detailed traces and benchmark tables in
the linked records.
