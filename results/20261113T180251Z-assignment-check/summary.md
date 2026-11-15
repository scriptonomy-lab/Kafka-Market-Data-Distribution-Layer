# Assignment check — 20261113T180251Z-assignment-check

Kafka's cooperative-sticky assignor on throw-away topics with topology.toml's
partition counts; Storage's consumer settings. Not market data.

**5 of 5 trials passed.**

| Trial | Step | Partitions per member | With data per member | Every member has data |
| --- | --- | --- | --- | --- |
| 1 | 2 members | {'a': 9, 'b': 9} | {'a': 3, 'b': 5} | True |
| 1 | 3 members (c joined) | {'a': 6, 'b': 6, 'c': 6} | {'a': 2, 'b': 4, 'c': 2} | True |
| 1 | 2 members (a left) | {'b': 9, 'c': 9} | {'b': 4, 'c': 4} | True |
| 2 | 2 members | {'a': 9, 'b': 9} | {'a': 3, 'b': 5} | True |
| 2 | 3 members (c joined) | {'a': 6, 'b': 6, 'c': 6} | {'a': 2, 'b': 4, 'c': 2} | True |
| 2 | 2 members (a left) | {'b': 9, 'c': 9} | {'b': 4, 'c': 4} | True |
| 3 | 2 members | {'a': 9, 'b': 9} | {'a': 3, 'b': 5} | True |
| 3 | 3 members (c joined) | {'a': 6, 'b': 6, 'c': 6} | {'a': 2, 'b': 4, 'c': 2} | True |
| 3 | 2 members (a left) | {'b': 9, 'c': 9} | {'b': 4, 'c': 4} | True |
| 4 | 2 members | {'a': 9, 'b': 9} | {'a': 3, 'b': 5} | True |
| 4 | 3 members (c joined) | {'a': 6, 'b': 6, 'c': 6} | {'a': 2, 'b': 4, 'c': 2} | True |
| 4 | 2 members (a left) | {'b': 9, 'c': 9} | {'b': 4, 'c': 4} | True |
| 5 | 2 members | {'a': 9, 'b': 9} | {'a': 3, 'b': 5} | True |
| 5 | 3 members (c joined) | {'a': 6, 'b': 6, 'c': 6} | {'a': 2, 'b': 4, 'c': 2} | True |
| 5 | 2 members (a left) | {'b': 9, 'c': 9} | {'b': 4, 'c': 4} | True |
