# Sync With LogDB

## Quick Start

First, get a server going.  I don't have a good description of this. Hopefully I'll get a hosted version up sometime.

Next, include this in your page:

    <script src="server-location/syncclient.js"></script>
    <script src="https://browserid.org/include.js"></script>

Now, create an object to integrate with your stored data:

    var MyAppData = {
      getPendingObjects: function (callback) {
        var result = [];
        for (object in objectsThatArentSaved) {
          result.push({id: object.id, data: object});
        }
        for (id in objectsThatWereDeleted) {
          result.push({id: id, deleted: true});
        }
        callback(result);
      },
      objectsSaved: function (objects) {
        objects.forEach(function (object) {
          if (object.deleted) {
            confirmDelete(object.id);
          } else {
            confirmSaved(object.data);
          }
        });
      },
      objectsReceived: function (objects) {
        objects.forEach(function (object) {
          if (object.deleted) {
            deleteObject(object.id);
          } else {
            createObject(object.data);
          }
        });
      },
      onupdate: function () {}
    };
    sync = new Sync(MyAppData);
    sync.watch({
      onlogin: function (email) {
        $('#login').text(email);
      },
      onlogout: function (email) {
        $('#login').text('login');
      }
    });
    $('#login').click(function () {
      if ($('#login').text() == 'login') {
        sync.request();
      } else {
        sync.logout();
      }
    });

    // and call MyAppData.onupdate() whenever you update your objects

That's it!  You also have a login system!

## Library

The library is present in `syncclient.js`.  While there are several internals that you could poke around with, the "simple" version is just one function, `new Sync(appData, {options})` which returns a sync object.

### appData

This is the object that tells the sync service how to interact with your data.  This object has three important methods:

#### `appData.getPendingObjects(callback)`

This should return all the *new* objects in your application.  You have to keep track yourself of when objects have been saved before, or if they've been updated, or if they've been deleted.

Your function should call `callback(null, [objects])` (the objects could be an empty list).  The return value does not matter.

Each object should look like:

    {
      id: "some stable id",
      type: "type-of-object",
      data: {some data}
    }

The `id` is something you should make yourself, but it should distinguish between updates and new objects.  Creating an ID with a UUID is perfectly fine, so long as you make that ID persistent.  The id should be a string or integer.

The `type` is just the kind of the object.  It should be a string.

The `data` is the actual data.  Some JSONable object.

If you have a deleted object, you should represent it like:

    {
      id: "some stable id",
      type: "type-of-object",
      deleted: true
    }

Note that having returned these objects, they may not get used!


#### `appData.objectsSaved(objects)`

This method indicates that the objects (as returned from `getPendingObjects` have actually been saved.  You should store something with the objects showing that they are saved, so you don't return them in future calls to `getPendingObjects`.

Note that attempts to upload objects can fail, so the results of `getPendingObjects` may be discarded and retrieved later for another attempt.  Typically such a case will result in a sequence of getPendingObjects, objectsReceived, getPendingObjects again, and finally objectsSaved.

#### `appData.objectsReceived(objects)`

This happens when a new object has appeared on the server.  The objects are formatted just as above, and probably came from another instance of your application.

These objects can include updates, new objects, and deletes.

#### `appData.status(message)`

This not-really-documented optional method is called to indicate things that are happening with the sync service.  You could maybe use it to indicate the sync status in the UI somewhere.

#### `appData.resetSaved()`

This is called when the server is switched out somehow, and you should erase all information about what objects have been saved in the past. This is done before starting a new from-scratch sync.

#### `appData.reportObjectErrors()`

This optional method is called if `getPendingObjects` returns a bad object.

#### `appData.onupdate`

This attribute will be set by the sync service, so long as the attribute exists (if you don't define it, it won't be set - even setting it to `null` is sufficient).  You are encouraged, but not required, to call this when the data changes.  This will cause the sync process to trigger soon, updating the server with the new data.


### options

There are a couple options to `Sync(appData, {options})`:

`assertion`: a browerid/persona assertion to login with.  If you don't provide this, then the service is started in a potentially not-logged-in state and you may have to call `navigator.id.request()` to trigger a login.

`appName`: this is the name of your app.  This is important only if your domain has multiple distinct apps.


### Sync object

The object returned as some methods:

`lastSyncTime()`: returns the timestamp (milliseconds) of the last successful sync.

`scheduleImmediately()`: try to do a sync right away.

`resetSchedule()`: reset the period polling to a normal pace.

`scheduleSlowly()`: slow down the polling.

`activate()/deactivate()`: activate or deactivate the scheduler. Before you retire a sync object you should call `sync.deactivate()`. The scheduler is automatically activated on startup.  The polling frequency is automatically adjusted based on whether the tab is in the background or not.

`logout()`: get rid of all authentication information.

`watch(options)`: similar to `navigator.id.watch()` but is called after server verification is done.  The `onlogin` method is called with `onlogin(email, completeData)` (not an assertion).  Note this uses cached credentials, unlike `navigator.id.watch()`.

`reset(callback)`: forgets all information about synchronization. This doesn't get rid of anything on the server, but does cause the next sync to start from scratch.


## Protocol

### Expectations

The model of sync is a stream of updates.  All clients both put their local updates into this stream, and read the collective stream. Everything has to be represented as a concrete item in the stream, meaning that delete actions are also present in the stream.

There is no conflict resolution, so clients must make sure they do not overwrite each other's updates.  If a conflict cannot be resolved without interaction (e.g., simple overwrite is not considered acceptable, and automatic merging is not possible) then it must be possible to represent the conflicted state directly, and at some point some client can resolve the conflict (possibly with user interaction) and put the unconflicted object into the stream.

The stream is ordered, along a single timeline.  The timeline markers should *not* be seen as based on any time or clock, as this leads to confusion and it's not clear whose "now" we are talking about. Instead the server has a counter, and all clients work from that counter. (The counter need not be an uninterrupted stream of integers, just increasing.)

All interaction between client and server should happen without user intervention.  Everything is expected to be highly asynchronous, and the server may reject requests or be unavailable for short periods of time, and this should not affect user experience.

We expect for a new client to be able to create a good-enough duplicate of the data in other clients.  "Good-enough" because some data might be kept by clients but expired by the server because it was marked as not being permanently interesting.

**TODO:** For "known" datatypes the sync server ensures the integrity of data, according to the most up-to-date notion of correctness for the data type.  As such the sync server must be updated frequently, but clients will be protected from some other rogue clients. ('''Note:''' not sure if this is a practical expectation?)

We'll go out-of-order time-wise, and forget about authentication for now.

Everything happens at a single URL endpoint, we'll call it `/USER`

### Objects

Each object looks like this:

    {type: "type_name",
     id: "unique identifier among objects of this type",
     expires: timestamp,
     data: {the thing itself}
    }

Note the `data` can be any JSONable object, including a string.

**TODO:** The `expires` key is entirely optional, and allows the server to delete the item (if it has not otherwise been updated).

The `id` key must be unique for the type (submitting another key by the same id means that you are overwriting that object).

You can also have a deleted object, which lives as an object in sync but doesn't have any data:

    {type: "type_name",
     id: "unique identifier",
     expires: timestamp,
     deleted: true
    }

### Requests

You can retrieve and send updates.  The first time is simple, you just want to accept whatever the server has: can just do:

    GET /USER

This returns the response document:

    {collection_id: "string_id",
     objects: [[counter1, object1], [counter2, object2]]
    }

The `collection_id` key is only in there because it was not sent with the request; it's a kind of "hello".

Subsequent requests look like:

    GET /USER?since=counter2&collection_id=string_id

You get the objects back, but with no `collection_id` (you already know it!)  If there have been no changes you get back a `204 No Content` response.

If `objects` is empty, you start with a counter `0`.

If the collection has changed, and your `string_id` doesn't match the server anymore, then you'll get:

    {collection_changed: true,
     collection_id: "new_id",
     objects: [[counter1, ...], ...]
    }

You should then forget your remembered `since` value and all the updates you have sent to the server.  This signals that whatever server or data you were communicating with before is gone.

When you have updates you want to send, you do:

    POST /USER?since=counter2&collection_id=string_id

    [{id: "my obj1", type: "thingy", data: {...}, ...]

This may return a `collection_changed` error, but also there may have been an update since you last retrieved objects.  This will not do! The `since=counter2` shows when you last did a GET. If there have been updates you get a new GET-like response:

    {since_invalid: true,
     objects: [[counter3, object]]
    }

You should incorporate the new object (which might conflict some with your own objects, which is why we do all this!), and then resubmit the request:

    POST /USER?since=counter3&collection_id=string_id
    ...

A successful response will be:

    {object_counters: [counter4, counter5, ...]}

The counters will correspond to each item that you sent.  You should keep the highest counter as your `since` value.  (**Note:** maybe this should include a timestamp of sorts too?)



#### Conflicts

We do not resolve conflicts as part of sync, and you are strongly recommended not to burden your users with conflicts as part of your sync schedule.

In some cases you can resolve conflicts yourself.  For instance, if the data is not very interesting, you can just choose a winner.

If you can't automatically resolve the conflicts you must incorporate all your conflicting edits into a new object, and when the user at some point can attend to the object you can show them the conflicts and ask for a resolution, putting the resolved object onto the server.


#### Partial Results

You may not want too many results.  In this case add to your GET requests:

    GET /USER?...&limit=10

This will return at most 10 items.  The server may also choose not to return a full set of items.  In either case the result object will have `incomplete: true`.  You can make another request and get more items.

#### Typed Results

Sometimes you only care about a subset of objects.  The stream can have any number of types of objects, and while a full client may handle everything a more limited client may not care about some items. In this case do:

    GET /USER?...&include=type1&include=type2

This gives you only `type1` and `type2` objects. Instead of opting in to some objects, you can also opt-out with `exclude=type1&exclude=type2`.

The response may include `until: "counter3"`, which might be newer than the newest item that was returned (this happens when the newest item is not of the type you requested).

You may also include these same filters on your POST requests; this keeps a conflict from happening even if an object of an excluded type has been added.


### Server Failure and Backoff

The server may return a 503 response, with a `Retry-After` value.  In any request it may also reply with `X-Sync-Poll-Time`, which is appended to a successful request but requests that you not make another request for the given time (in seconds).

### Authentication

Each request has to have authentication.  The authentication uses BrowserID.  To get authentication information you make a request to:

    POST /DB/verify

    assertion=...&audience=...

This will return a JSON response that will indicate how to authenticate future requests, like:

    {
      "email": "user@example.com",
      "auth": {
        "query": {"auth": "auth_string"}
      }
    }

What is in the `"auth"` key determines what you should do to authenticate future requests.  401 responses indicate you should re-authenticate with a new assertion.

## Clients

The client algorithm is to get and put updates, storing them locally. That easy?  Sure!

### GET

The client needs to keep a record of these values:

* The `since` counter
* The `collection_id`
* Authentication

The `since` counter and the `collection_id` go together to point to where in the stream of updates the client is.  If the collection changes, that counter becomes meaningless, hence the `collection_id` - and when you get a `collection_changed` response you should forget your `since` value.

Every time you get a response, you update the `since` value if there were updates.  If `until` is set on the response, use that, otherwise use the counter from the last item in `objects`.  If you get no objects, no until, or a 204 No Content respones, then don't change anything.

You should keep getting stuff so long as the response includes `incomplete: true`.  Also Retry-After and X-Sync-Poll-Time should inform the speed at which you make requests.

### POST

Once you've retrieved the values, then you can send your own new values.  You'll send `?since={since}` just like with GET, because you must always incorporate every value before sending your own.  This ensures that anyone who adds to the sync timeline is fully aware of everything preceding.

The POST results, when successful, also update `since`.  And the POST results when unsuccessful look just like a GET (since you needed to do a GET, right?)

#### Quarantine

Sometimes you may not understand an object you receive; its type, or the format it is in.  This might be because of corruption, but it might also be because another client has a newer/richer notion of the type than you do.
