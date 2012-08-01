A tiered approach to implementing load balancing, failover, and replication with logdb.sync:

1. Start with simple balancing: given a set of N fixed nodes, balance requests among them.  Nodes are expected to be stable.

2. Add nodes to the system.

3. Remove a node from the system, gracefully.  This is not a node falling off the system, but one that is taken off while still working, and allowed to gracefully retire itself.

4. Keeping a live backup of one node to the other.  This will be a regression from 2 and 3 (nodes will become static again).

5. Add a new node, taking over some primary and some backup services from other nodes.

6. Remove a node (not suddenly).

7. Make the number of backups dynamic (e.g., two backups).

8. Remove a node suddenly/ungracefully.
