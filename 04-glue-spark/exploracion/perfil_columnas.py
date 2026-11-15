#!/usr/bin/env python3
import csv, glob, re, collections

cols = collections.defaultdict(collections.Counter)
for f in sorted(glob.glob("nacimientos_*.csv")):
    with open(f, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.DictReader(fh):
            for k, v in row.items():
                if k is not None:
                    cols[k][v] += 1

for k, c in cols.items():
    nueves = {v: n for v, n in c.items() if re.fullmatch(r"9+", v)}
    print(f"{k:16} distintos={len(c):6} vacios={c.get('', 0):7} candidatos={nueves}")