## The Cut-Out & sync

This repo contains a few separate components, all of which work together to implement a system for syncing data from a smart client app (i.e., an application that uses browser-based persistence like [IndexedDB](https://developer.mozilla.org/en-US/docs/IndexedDB)) to a server, and to other instances of that application.

The system uses a time series of data to handle this synchronization, where clients append new objects and updates to the series, and other clients get those updates.  A simple form of garbage collection is used to trim obsolete updates.  Tombstone objects (i.e., null objects with a deleted status) are used to represent deletions.

The system has been designed as a unit, with development of the Javascript client, server protocol, and on-disk representation all aligned to this specific purpose.

### Javascript client

The code for the client is in [syncclient.js](/ianb/thecutout/blob/master/cutout/syncclient.js).

This client handles server interaction and the state of the sync process.  It doesn't touch application persistence directly, instead some glue must be written to handle incoming objects and track objects that have been saved or not.  The library is entirely UI-neutral and model-neutral.

The authentication is handled by [Persona/BrowserID](https://login.persona.org/).

The library is [documented here](https://github.com/ianb/thecutout/blob/master/sync.md#library).

### HTTP Protocol

The protocol is [described in detail here](https://github.com/ianb/thecutout/blob/master/sync.md#protocol).

Authentication happens by posting to `/verify` with the BrowserID assertion and audience, must as you'd submit a request to the BrowserID verifier.  You get back some information on how to authenticate future requests.

Then there is really just two verbs: GET and POST.  To poll for updates, do a GET.  You'll get back a list of objects, and a server time-sequence.  On later requests you'll pass that time sequence back, and only get updates since that time.

To put updates, you POST a request with the updated objects, including an indication of when you last got updates - if there are updates you haven't seen then the submission will fail.

The services is intended to support [Cross-Origin requests](https://developer.mozilla.org/en-US/docs/HTTP_access_control) so that the system can be hosted centrally for applications hosted elsewhere.

### On-disk representation

The time sequence is stored on disk with the [The Cut-Out database](/ianb/thecutout/blob/master/cutout/__init__.py)

This is a very simple database with just an index file and a file that stores a sequence of blobs.  It only supports searching by the time sequence index - to find a particular object you'd have to scan through the entire file.

There is a separate database file (plus index) for each user and each application.  Files are not kept open - when a database isn't being accessed it is simply a file on disk.

### Load balancing and replication

A fairly naive balancing and replication system is in [balancer.py](/ianb/thecutout/blob/master/cutout/balancer.py).

This uses [consistent hashing](http://en.wikipedia.org/wiki/Consistent_hashing) to map requests to nodes.  The node is also asked to forward these requests on to one or more backup nodes.

When a node is added to or removed from the system the balancer sends a request to the node to handle the rearrangement of databases (or in the case of a node disappearing, all other nodes are asked to take up the slack).  No one host is a replacement for any single other node so the nodes must chat between each other a great deal during these operations.  A reasonable setup would use sharding among a stable number of pools, and inside those pools the balancer would be used to do balancing and replication among the nodes in that smaller pool.

Right now concurrency is not handled well at several levels of the system.  However, it's not unreasonable to do locking at several levels, and requests can be rejected with no real effect on user experience, so this gives a lot of opportunity to apply fairly widespread locks to protect concurrent access.  Given likely usage scenarios, this should have no effect on normal use.

At several levels diabolic clients could cause problems.

### Data portability

The general pattern I think has some peculiar but positive implications for data portability.  None of this helps you keep data portable on the clients themselves.  That's up to you, though it does require you to implement thoughtful serialization which is always a good first step to portability.

But one interesting note is that the server representation needn't be singular.  You can hook the same application up to multiple sync systems - and specifically, you can upgrade your online object representation by starting to use a sync storage system (aka "bucket") and objects will automatically start to be moved to that old system, while still supporting the old system; and you just remove the adapter for the old system once you feel everything is okay.
