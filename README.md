[![CircleCI](https://circleci.com/gh/casualjim/go-rapid.svg?style=svg)](https://circleci.com/gh/casualjim/go-rapid) [![codecov](https://codecov.io/gh/casualjim/go-rapid/branch/master/graph/badge.svg)](https://codecov.io/gh/casualjim/go-rapid)

What is Rapid?
==============

Rapid is a distributed membership service. It allows a set of processes to
easily form clusters and receive notifications when the membership changes.

We observe that datacenter failure scenarios are not always crash failures, but
commonly involve misconfigured firewalls, one-way connectivity loss, flip-flops
in reachability, and some-but-not-all packets being dropped. However, existing
membership solutions struggle with these common failure scenarios, despite
being able to cleanly detect crash faults. In particular, existing tools take
long to, or never converge to, a stable state where the faulty processes are
removed.

To address the above challenge, we present Rapid, a scalable, distributed
membership system that is stable in the face of a diverse range of failure
scenarios, and provides participating processes a strongly consistent view of
the system's membership.



How does Rapid work?
====================

Rapid achieves its goals through the following three building blocks:

* **Expander-based monitoring edge overlay.** To scale monitoring load, Rapid
organizes a set of processes (a *configuration*) into a stable failure
detection topology comprising *observers* that monitor and disseminate reports
about their communication edges to their *subjects*.  The monitoring
relationships between processes forms a directed expander graph with strong
connectivity properties, which ensures with a high probability that healthy
processes detect failures.  We interpret multiple reports about a subject's
edges as a high-fidelity signal that the subject is faulty.

* **Multi-process cut detection.** For stability, processes in Rapid (i)
suspect a faulty process *p* only upon receiving alerts from multiple observers
of *p*, and (ii) delay acting on alerts about different processes until the
churn stabilizes, thereby converging to detect a global, possibly multi-node
*cut* of processes to add or remove from the membership.  This filter is
remarkably simple to implement, yet it suffices by itself to achieve
*almost-everywhere agreement* -- unanimity among a large fraction of processes
about the detected cut.

* **Practical consensus.** For consistency, we show that converting
almost-everywhere agreement into full agreement is practical even in
large-scale settings. Rapid's consensus protocol drives configuration changes
by a low-overhead, leaderless protocol in the common case: every process simply
validates consensus by counting the number of identical cut detections.  If
there is a quorum containing three-quarters of the membership set with the same
cut, then without a leader or further communication, this is a safe consensus
decision.


Pluggable failure detectors
===========================

A powerful feature of Rapid is that it allows users to use custom failure
detectors. By design, users inform Rapid how an observer *o* can announce its
monitoring edge to a subject *s* as faulty by implementing a simple interface
(`api.Detector`). Rapid builds the expander-based monitoring
overlay using the user-supplied template for a monitoring edge.


Pluggable messaging
===================

When embedding a membership service in a larger system, there is no reason
for the membership service to use its own messaging implementation. For
this reason, Rapid also allows users to plugin their own messaging 
implementations by implementing two interfaces, `api.Client` and
`api.Server`. 


Where can I read more?
======================

We suggest you start with our [USENIX ATC 2018 paper](https://www.usenix.org/conference/atc18/presentation/suresh).
The paper and an accompanying tech report are both available in the `docs` folder.

