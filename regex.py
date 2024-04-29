import re

s = "created 1961 (EC 1.1.1.74 created 1972, incorporated 1976)"
pattern = "created ([0-9]*)"
match = re.search(pattern=pattern, string=s)

if match:
    created = match.group(1)
    print(created)
else:
    print("No match found")
