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

Keep the [roadmap](/latest/roadmap/) current as work lands. It should be the
short checkpoint for what changed recently and what remains pending, even when
the detailed docs for a feature live elsewhere.
