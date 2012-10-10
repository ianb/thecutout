jshint('syncclient.js', {laxbreak: true, shadow: true});
// => Script passed: .../syncclient.js

var user;
print(user = "test-"+(new Date().getTime())+"@example.com");
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
print(server);
// => [...]

Spy.defaultOptions.wrapArgs = true;

/****************************************
Now we'll just do a bit of mock authentication.  We use a mock
BrowserID assertion that indicates exactly who we want to log into.
It also supports ?a={audience} which indicates what
audience the assertion is valid for.  This looks useful, but is
actually only useful for one thing: allows us to cause an invalid
audience ;)
*/

/****************************************
** Interacting with the server
****************************************/

// The first argument is 'since'; we've never gotten anything...
server.get(null, Spy('server.get', {wait: true}));
/* =>
server.get(null, {
  collection_id: "?",
  objects: []
})
*/

server.get('asdf', Spy('server.get-fail'));
// => Error: In get(since, ...) since must be a number or null, not: asdf

// 0 is basically the same as null...
server.get(0, Spy('server.get', {wait: true}));
/* =>
server.get(null, {
  collection_id: "?",
  objects: []
})
*/

// OK, let's do it for serious now:
var spy = Spy('server.get');
server.get(0, spy);
spy.wait();
/* =>
server.get(null, {
  collection_id: "?",
  objects: []
})
*/

var putSpy = Spy('server.put');
server.put(null, [{
  manifest: {name: "a fun app"},
  manifest_url: "http://example.com/manifest.webapp",
  origin: "http://example.com",
  install_data: null,
  install_origin: "http://store.example.com",
  install_time: 100
}], putSpy);
putSpy.wait();
/* =>
server.put(null, {
  collection_id: "?",
  object_counters: [1]
})
*/

// Now we should see that update:
server.get(0, spy=Spy('server.get', {wait: true}));
/* =>
server.get(null, {
  collection_id: "?",
  objects: [
    [
      1,
      {
        install_data: null,
        install_origin: "http://store.example.com",
        install_time: 100,
        manifest: {
          name: "a fun app"
        },
        manifest_url: "http://example.com/manifest.webapp",
        origin: "http://example.com"
      }
    ]
  ]
})
*/

var objects = spy.args[1].objects;
var until = objects[objects.length-1][0];
print(until);
// => ?
// But we won't see it if we have a later since time:
server.get(until+2, Spy('server.get', {wait: true}));
/* =>
server.get(null, {
  collection_id: "?",
  objects: []
})
*/
