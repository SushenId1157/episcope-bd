# Reflection — Rafiqul Islam

## My area

Performance measurement and automated testing.

## How it works, in my own words

*Question 1: Processing 28 million records on 2 CPUs takes 42–47 minutes. Which two steps are the slowest? (2 lines)*

Answer: The two slowest steps are encounter generation (exploding the weekly counts into about 28 million records) and the silver build (de-identification, the broadcast facility join, duplicate ranking, and 14 quality rules over all 28 million rows). The whole pipeline takes 41.9 minutes on 2 cores, and these two full-scan stages dominate that time; gold is small (about 82k rows) and is not the bottleneck.

## A decision and why

*Question 2: One attempt made the pipeline faster but changed the answer (99 extra rows). Why is that not acceptable? (2 lines)*

Answer: A faster version that changes the output is not an optimisation, it is a bug. The 99 extra rows mean the logic changed (a join, filter or grouping behaved differently), and a wrong answer is unacceptable no matter how fast it is, so correctness must come before speed and the change was reverted.

## What I learned

*Question 3: One test showed 8 green ticks, yet 5 million rows had been deleted. What did you learn from this? (2-3 lines)*

Answer: I learned that green tests do not prove correctness. The earlier test compared successive post-load snapshots, but the deleted 5,091,096 rows were equally missing from every snapshot, so the comparison showed nothing wrong. The fix was to assert against a pre-load baseline and independently known quantities, so a test checks that the result is correct, not just that the code ran.
