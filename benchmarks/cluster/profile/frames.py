#!/usr/bin/env python3
"""Who calls a hot function: frames.py perf-node-0.data [SYMBOL]

Lists the innermost Fibril, Stroma, Keratin or Ganglion frame of each sample,
optionally only for samples whose top frames contain SYMBOL (for example
serde_json). Needs a build with frame pointers and line tables.
"""
import re
import sys
from collections import Counter

from common import frames, samples

path = sys.argv[1]
match = sys.argv[2] if len(sys.argv) > 2 else None
ours = re.compile(r"(fibril|stroma|keratin|ganglion)")
counts, total = Counter(), 0
for lines in samples(path):
    total += 1
    names = frames(lines)
    if match and not any(match in n for n in names[:3]):
        continue
    project = [n for n in names if ours.search(n)]
    counts[re.sub(r"::h[0-9a-f]{16}$", "", project[0])[:140] if project else "(no project frame)"] += 1
print(f"{total} samples, {sum(counts.values())} matched")
for name, n in counts.most_common(20):
    print(f"{100 * n / total:6.2f}% {name}")
