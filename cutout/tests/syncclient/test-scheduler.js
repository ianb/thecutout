jshint('syncclient.js', {laxbreak: true, shadow: true});
// => Script passed: .../syncclient.js


/*
We'll setup the scheduler, with the service and repository.  The
server needs to be run with the client test overrides in place, so we
can trigger various cases and make sure the scheduling works.
*/

var server = new Sync.Server(location.hostname + '/' + mockUser, 'bucket', Authenticator);
var appData = new MockAppData('appData');
var storage = new Sync.LocalStorage('sync::');
storage.clear();
var service = new Sync.Service(server, appData, storage);
print(service);
// => [Sync.Service server: ... appData: ...]

var scheduler = new Sync.Scheduler(service, Authenticator);

function override(t, data) {
  var done = false;
  wait(function () {return done;});
  var req = new XMLHttpRequest();
  if (! data) {
    req.open('DELETE', '/' + t);
  } else {
    req.open('POST', '/' + t);
  }
  req.onreadystatechange = function () {
    if (req.readyState == 4) {
      writeln('Completed');
      done = true;
    }
  };
  if (data) {
    data = JSON.stringify(data);
  } else {
    data = null;
  }
  req.send(data);
}

scheduler.schedule = function () {
};


function status() {
  console.log('sched', scheduler._period, scheduler._periodAddition);
  var extra = '';
  if (scheduler._periodAddition) {
    if (scheduler._periodAddition < 0) {
      extra = ' ' + scheduler._periodAddition;
    } else {
      extra = ' +' + scheduler._periodAddition;
    }
  }
  print('Poll: ' + scheduler._period + extra);
}

function get() {
  var done = false;
  server.get(null, function () {
    done = true;
    status();
    // This gets regularly reset anyway:
    scheduler._periodAddition = 0;
  });
  wait(function () {return done;});
}


/****************************************
*/

print(scheduler.settings);
/* =>
{
  failureIncrease: 300000,
  immediateUpdateDelay: 500,
  maxPeriod: 3600000,
  minPeriod: 30000,
  normalPeriod: 300000
}
*/

// This will start out with the standard poll time
get();
/* =>
Poll: 300000
appData.status: {status: "sync_get", timestamp: ?}
appData.getPendingObjects: []
*/

// Immediate schedule should mean a minimal update time
scheduler.scheduleImmediately();
status();
// => Poll: 300000 -299500

// And undo it
scheduler._periodAddition = 0;
override('__testing__', {status: 503});
// => Completed

// This should be normalPeriod + failureIncrease
get();
// => Poll: 600000

override('__testing__', {status: 503, headers: {"Retry-After": 700}});
// => Completed

// This should be the retry-after time (in milliseconds); note we keep the
// normal period but use ad addition
get();
// => Poll: 600000 +100000

override('__testing__', {status: 200, headers: {"Retry-After": 500}});
// => Completed

get();
// => Poll: 600000 -100000

scheduler.resetSchedule();
// Now it should get reset to the normal period
status();
// => Poll: 300000

override('__testing__', {status: 503, headers: {"X-Sync-Poll-Time": 2}});
// => Completed

// Despite everything, we should keep a certain minimum
get();
// => Poll: 300000 -270000

override('__testing__', {status: 500});
// => Completed
// Now a standard increase
get();
// => Poll: 600000

scheduler.resetSchedule();
// Back to regular again
status();
// => Poll: 300000

override('__testing__', {status: 200, headers: {"X-Sync-Poll-Time": 400}});
// => Completed

get();
// => Poll: 300000 +100000

override('__testing__', {status: 200, headers: {"X-Sync-Poll-Time": "NaN"}});
// => Completed

// With an invalid time, we should ignore it
get();
// => Poll: 300000

var date = (new Date(Date.now() + 10000));
print(date.toString());
// => ...

override('__testing__', {status: 503, headers: {"Retry-After": date}});
// => Completed

// The date should get parsed; but we'll end up with minPeriod
get();
// => Poll: 300000 -270000


/****************************************
Next, we'll give a quick try at triggering logout/auth:
*/

print(Authenticator.loggedIn());
// => true
override('__testing__', {status: 401});
// => Completed
get();
// => ...
print(Authenticator.loggedIn());
// => false
