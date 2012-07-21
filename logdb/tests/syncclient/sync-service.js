jshint('syncclient.js', {laxbreak: true, shadow: true});
// => Script passed: .../syncclient.js

/****************************************
 We'll setup the service.  Note that the service goes between a
 server and a repo.  We have a "real" mock server, but the repo
 (MockRepo) is just defined inline (view source to see
 it).
 */

var user;
print(user = "test-"+(Date.now())+"@example.com");
// => test-?@example.com

var domain = location.hostname;
print(domain);
// => ...

var Authenticator = {
  modifyRequest: function (req) {
    req.setRequestHeader('X-Remote-User', user + '/' + domain);
  },

  loggedIn: function () {
    return true;
  },

  onlogin: null,
  onlogout: null
};

var serverUrl = doctest.params.server ||
  "/" + encodeURIComponent(domain) +
  '/' + encodeURIComponent(user) + "/bucket";
var server = new Sync.Server(serverUrl, Authenticator);
server.XMLHttpRequest = doctest.NosyXMLHttpRequest.factory('ServerReq');
print(server);
// => [...]

function MockAppData(name) {
  this._name = name;
  this._objectsById = {};
  this._dirtyObjects = {};
  this._cleanObjects = {};
  this._deletedObjects = {};
}

MockAppData.prototype = {
  _addObject: function (object) {
    if (! object.id) {
      throw 'All objects must have ids';
    } // FIXME: and type?
    this._objectsById[object.id] = object;
    this._dirtyObjects[object.id] = object;
    if (object.id in this._cleanObjects) {
      delete this._cleanObjects[object.id];
    }
  },

  _deleteObject: function (id) {
    delete this._objectsById[id];
    delete this._dirtyObjects[id];
    delete this._cleanObjects[id];
    this._deletedObjects[id] = true;
  },

  getPendingObjects: function (callback) {
    var allObjects = [];
    for (var i in this._dirtyObjects) {
      allObjects.push(this._dirtyObjects[i]);
    }
    for (i in this._deletedObjects) {
      allObjects.push({id: i, deleted: true});
    }
    print(this._name + '.getPendingObjects:', allObjects);
    if (callback) callback(null, allObjects);
  },

  objectsSaved: function (objects, callback) {
    print(this._name + '.objectsSaved(' + repr(objects) + ')');
    var errors = [];
    for (var i=0; i<objects.length; i++) {
      var object = objects[i];
      if (object.deleted) {
        if (! (object.id in this._deletedObjects)) {
          print('  Deleted object', repr(id), 'does not exist');
        }
        errors.push({object: object, error: 'deleted object does not exist'});
      } else {
        if (! (object.id in this._objectsById)) {
          print('  Object does not exist:', repr(object.id));
          errors.push({object: object, error: 'Object does not exist'});
        } else if (! (object.id in this._dirtyObjects)) {
          print('  Object not marked dirty:', repr(object.id));
          errors.push({object: object, error: 'Object not marked dirty'});
        }
        this._cleanObjects[object.id] = this._objectsById[object.id];
        delete this._dirtyObjects[object.id];
      }
    }
    if (! errors.length) {
      errors = null;
    }
    if (callback) callback(errors);
  },

  objectsReceived: function (objects, callback) {
    print(this._name + '.objectsReceived:', objects);
    for (var i=0; i<objects.length; i++) {
      var object = objects[i];
      if (object.deleted) {
        if (object.id in this._objectsById) {
          print('  deleting object:', object.id);
        } else {
          print('  obsolete object received:', object.id);
        }
        delete this._objectsById[object.id];
        delete this._cleanObjects[object.id];
        delete this._dirtyObjects[object.id];
      } else {
        if (object.id in this._objectsById) {
          print('  overwriting object:', object.id);
        } else {
          print('  creating object:', object.id);
        }
        this._objectsById[object.id] = this._cleanObjects[object.id] = object;
      }
    }
    if (callback) callback();
  },

  resetSaved: function () {
    print(this._name + '.resetSaved()');
    this._dirtyObjects = {};
    this._deletedObjects = {};
    this._cleanObjects = {};
    for (var i in this._objectsById) {
      this._dirtyObjects[i] = this._objectsById[i];
    }
  },

  reportObjectErrors: function (errors) {
    for (var i=0; i<errors.length; i++) {
      print('Object error:', errors[i]);
    }
  },

  status: function (message) {
    print(this._name + '.status:', message);
  }

};

var appData = new MockAppData('appData');
(new Sync.LocalStorage('sync1::')).clear();
var service = new Sync.Service({
  server: server,
  appData: appData,
  localStoragePrefix: 'sync1::'
});
print(service);
// => [Sync.Service server: ... appData: ...]

var appData2 = new MockAppData('appData2');
(new Sync.LocalStorage('sync2::')).clear();
var service2 = new Sync.Service({
  server: server,
  appData: appData2,
  localStoragePrefix: 'sync2::'
});

/****************************************
 Next we'll try installing applications into the repository, and then
 poke the sync service to get it to sync them to the server.
*/

print(service.lastSyncTime());
// => 0

service.syncNow(Spy('service.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=0")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service.syncNow()
*/

print(service.lastSyncTime() > 0, service.lastSyncTime());
// => true ?

appData._addObject({id: 'aaa', data: 1});
service.syncNow(Spy('service.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=0")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: [{data: 1, id: "aaa"}]
appData.status: {count: 1, status: "sync_put", timestamp: ?}
ServerReq.open("POST", ".../bucket?since=0")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"aaa\",\"data\":1}]")
appData.status: {status: "sync_put_complete", timestamp: ?}
appData.objectsSaved([{data: 1, id: "aaa"}])
service.syncNow()
*/

// Now we confirm that the update was received by the server
server.get(null, Spy('server.get', {wait: 2000}));
/* =>
ServerReq.open("GET", ".../bucket?since=0")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
server.get(null, {collection_id: "?", objects: [[1, {data: 1, id: "aaa"}]]})
*/

service2.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=0")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{data: 1, id: "aaa"}]
  overwriting object: aaa
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service2.syncNow()
*/

// We just use a really large since value to avoid a conflict:
server.put(100, [{id: 'bbb', data: 2}],
           Spy('server.put', {wait: 5000}));
/* =>
ServerReq.open("POST", ".../bucket?since=100")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"bbb\",\"data\":2}]")
server.put(null, {collection_id: "?", object_counters: [2]})
*/

service.syncNow(Spy('service.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=1")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{data: 2, id: "bbb"}]
  creating object: bbb
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service.syncNow()
*/

service2.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=1")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{data: 2, id: "bbb"}]
  overwriting object: bbb
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service2.syncNow()
*/

server.put(100, [{id: 'aaa', deleted: true}],
           Spy('server.put', {wait: 5000}));
/* =>
ServerReq.open("POST", ".../bucket?since=100")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"aaa\",\"deleted\":true}]")
server.put(null, {collection_id: "?", object_counters: [3]})
*/

service.syncNow(Spy('service.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=2")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{deleted: true, id: "aaa"}]
  deleting object: aaa
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service.syncNow()
*/

service2.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=2")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{deleted: true, id: "aaa"}]
  obsolete object received: aaa
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service2.syncNow()
*/

appData._addObject({id: 'ccc', data: 3});
service.syncNow(Spy('service.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=3")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: [{data: 3, id: "ccc"}]
appData.status: {count: 1, status: "sync_put", timestamp: ?}
ServerReq.open("POST", ".../bucket?since=3")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"ccc\",\"data\":3}]")
appData.status: {status: "sync_put_complete", timestamp: ?}
appData.objectsSaved([{data: 3, id: "ccc"}])
service.syncNow()
*/

service2.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=3")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{data: 3, id: "ccc"}]
  overwriting object: ccc
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
service2.syncNow()
*/

server.get(service2._syncPosition - 1, Spy('server.get', {wait: 2000}));
/* =>
ServerReq.open("GET", ".../bucket?since=3")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
server.get(null, {collection_id: "?", objects: [[4, {data: 3, id: "ccc"}]]})
*/

appData._deleteObject('aaa');
service.syncNow(Spy('service.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=4")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: [{deleted: true, id: "aaa"}]
appData.status: {count: 1, status: "sync_put", timestamp: ?}
ServerReq.open("POST", ".../bucket?since=4")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"aaa\",\"deleted\":true}]")
appData.status: {status: "sync_put_complete", timestamp: ?}
appData.objectsSaved([{deleted: true, id: "aaa"}])
service.syncNow()
*/

server.get(service._syncPosition - 1, Spy('server.get', {wait: 2000}));
/* =>
ServerReq.open("GET", ".../bucket?since=4")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
server.get(null, {collection_id: "?", objects: [[5, {deleted: true, id: "aaa"}]]})
*/

service2.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=4")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData.objectsReceived: [{deleted: true, id: "aaa"}]
  obsolete object received: aaa
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: [{deleted: true, id: "aaa"}]
appData.status: {count: 1, status: "sync_put", timestamp: ?}
ServerReq.open("POST", ".../bucket?since=5")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"aaa\",\"deleted\":true}]")
appData.status: {status: "sync_put_complete", timestamp: ?}
appData.objectsSaved([{deleted: true, id: "aaa"}])
service2.syncNow()
*/
