jshint('syncclient.js', {laxbreak: true, shadow: true});
// => Script passed: .../syncclient.js

/****************************************
 We'll setup the service.  Note that the service goes between a
 server and a repo.  We have a "real" mock server, but the repo
 (MockRepo) is just defined inline (view source to see
 it).
 */

var server = new Sync.Server(location.hostname + '/' + mockUser, 'bucket', Authenticator);
server.XMLHttpRequest = doctest.NosyXMLHttpRequest.factory('ServerReq');
print(server);
// => [...]

var appData = new MockAppData('appData');
var storage = new Sync.LocalStorage('sync1::');
storage.clear();
var service = new Sync.Service(server, appData, storage);
print(service);
// => [Sync.Service server: ... appData: ...]

var appData2 = new MockAppData('appData2');
var storage2 = new Sync.LocalStorage('sync2::');
storage2.clear();
var service2 = new Sync.Service(server, appData2, storage2);

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
server.get(null, {objects: [[1, {data: 1, id: "aaa"}]]})
*/

service2.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=0")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
appData2.objectsReceived: [{data: 1, id: "aaa"}]
  creating object: aaa
appData2.status: {status: "sync_get", timestamp: ?}
appData2.getPendingObjects: []
service2.syncNow()
*/

// We just use a really large since value to avoid a conflict:
server.put(100, [{id: 'bbb', data: 2}],
           Spy('server.put', {wait: 5000}));
/* =>
ServerReq.open("POST", ".../bucket?since=100")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"bbb\",\"data\":2}]")
server.put(null, {object_counters: [2]})
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
appData2.objectsReceived: [{data: 2, id: "bbb"}]
  creating object: bbb
appData2.status: {status: "sync_get", timestamp: ?}
appData2.getPendingObjects: []
service2.syncNow()
*/

server.put(100, [{id: 'aaa', deleted: true}],
           Spy('server.put', {wait: 5000}));
/* =>
ServerReq.open("POST", ".../bucket?since=100")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send("[{\"id\":\"aaa\",\"deleted\":true}]")
server.put(null, {object_counters: [3]})
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
appData2.objectsReceived: [{deleted: true, id: "aaa"}]
  deleting object: aaa
appData2.status: {status: "sync_get", timestamp: ?}
appData2.getPendingObjects: []
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
appData2.objectsReceived: [{data: 3, id: "ccc"}]
  creating object: ccc
appData2.status: {status: "sync_get", timestamp: ?}
appData2.getPendingObjects: []
service2.syncNow()
*/

server.get(service2._syncPosition - 1, Spy('server.get', {wait: 2000}));
/* =>
ServerReq.open("GET", ".../bucket?since=3")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
server.get(null, {objects: [[4, {data: 3, id: "ccc"}]]})
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
server.get(null, {objects: [[5, {deleted: true, id: "aaa"}]]})
*/

service.syncNow(Spy('service2.syncNow', {wait: 5000}));
/* =>
ServerReq.open("GET", ".../bucket?since=5")
ServerReq.setRequestHeader("X-Remote-User", "...")
ServerReq.send()
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
