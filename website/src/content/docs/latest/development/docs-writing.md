---
title: Documentation style
description: How Fibril docs should separate user-facing behavior from implementation detail.
---

Fibril documentation should start from the user's problem, not from the internal component that solves it.

Most user-facing pages should follow this shape:

1. What problem this addresses.
2. What Fibril does.
3. What conditions activate or block the behavior.
4. What tradeoffs or limits the user should expect.
5. Links to deeper implementation notes only when useful.

Avoid making regular concept pages depend on internal names such as queue actors, cached handles, or storage materialization unless the name is also part of the user-facing model.

Implementation detail is still useful, but it belongs in development notes. User-facing pages can link there with clear wording so readers know they are choosing a deeper design explanation, not the next required step.

Use the docs sections deliberately:

- `concepts`, `reliability`, `configuration`, `quickstart`, `clients`, and
  `admin-dashboard` should explain what users can do and what behavior they can
  rely on.
- `status` should be a concise user-facing feature matrix.
- `implemented-surface` can be more detailed, but should still say which client
  or operator path is wired before naming internals.
- `development` is the right place for implementation mechanisms, tradeoff
  records, and future design policies.

Keep the [roadmap](/latest/roadmap/) current as work lands. It should be the
short checkpoint for what changed recently and what remains pending, even when
the detailed docs for a feature live elsewhere.
