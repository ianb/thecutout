/* -*- Mode: JavaScript; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is trusted.js; substantial portions derived
 * from XAuth code originally produced by Meebo, Inc., and provided
 * under the Apache License, Version 2.0; see http://github.com/xauth/xauth
 *
 * Contributor(s):
 *     Ian Bicking <ianb@mozilla.com>
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

/* About sync.js:

There are three main objects defined in this module:

Sync.Server: handles the actual interaction with the server.  This
  handles the login process and stores the credentials, but otherwise
  has no state.

Sync.Service: handles actual synchronization, and keeps state about the
  sync progress.  This interacts with the server and the repo
  (including some private methods, not just navigator.mozApps APIs)

Sync.Scheduler: handles scheduling of calls to Sync.Service.  It should
  also respond to events from the server (like a Retry-After) and


Also:

Sync.LocalStorage: storage implementation that uses localStorage, for
keeping sync-related persistent metadata.


All functions here use Node-style error handling, where the functions
take a callback with the signature callback(error, result), where
error is null or undefined in case of success (result itself is
optional depending on the function).

*/

var Sync = {};

// Note, this URL gets rewritten by the server:
Sync.baseUrl = null;

// FIXME: change args to positional arguments
Sync.Service = function (args) {
  if (this === window) {
    throw 'You forgot new Sync.Service';
  }
  this.server = args.server;
  this.appData = appData;
  if (! this.appData) {
    throw 'You must provide an appData object';
  }
  this.storage = args.storage || new Sync.LocalStorage(args.localStoragePrefix || 'sync::');
  console.log('sent storage.get', this.storage);
  this.storage.get(
    ['lastSyncTime', 'lastSyncPut', 'lastSyncCollectionId', 'syncPosition'],
    (function (values) {
      console.log('got storage', values);
      this._lastSyncTime = values.lastSyncTime ? parseFloat(values.lastSyncTime) : null;
      this._lastSyncPut = values.lastSyncPut ? parseFloat(values.lastSyncPut) : null;
      this._lastSyncCollectionId = values.lastSyncCollectionId || null;
      this._syncPosition = values.syncPosition || 0;
    }).bind(this)
  );
  // This will get set if the server tells us to back off on polling:
  this._backoffTime = null;
};

Sync.Service.prototype = {
  toString:  function () {
    return '[Sync.Service server: ' + this.server + ' appData: ' + this.appData + ']';
  },

  /* Resets all state for the service; returns the service back to the state
     as if it has never interacted with any server */
  reset: function (callback) {
    var steps = 0;
    var allSet = false;
    done = (function () {
      steps--;
      if (steps === 0 && allSet) {
        this.sendStatus({status: 'reset'});
        return callback && callback();
      }
    }).bind(this);
    steps++;
    this.storage.put({
      lastSyncTime: null,
      lastSyncPut: null,
      lastSyncCollectionId: null,
      syncPosition: null
    }, done);
    this._lastSyncTime = null;
    this._lastSyncPut = null;
    this._syncPosition = 0;
    if (steps === 0) {
      this.sendStatus({status: 'reset'});
      return callback && callback();
    } else {
      allSet = true;
    }
    this.appData.resetSaved();
  },

  /* Sends a status message to any listener */
  sendStatus: function (message) {
    message.timestamp = new Date().getTime();
    if (this.appData.status) this.appData.status(message);
  },

  /* Confirms that the collection_id received from the server matches the uuid we
     expect; if it does not then it resets state and returns false.  Any
     syncing in process should be abandoned in this case, and the sync
     started over from the beginning. */
  confirmCollectionId: function (collectionId) {
    if (collectionId === undefined) {
      log('Undefined collectionId');
      return true;
    }
    log('confirming collectionId', collectionId || 'no remote', this._lastSyncCollectionId || 'no local');
    if ((! this._lastSyncCollection) && collectionId) {
      this._lastSyncCollectionId = collectionId;
      this.storage.put('lastSyncCollectionId', collectionId);
      return true;
    } else if (this._lastSyncCollectionId == collectionId) {
      return true;
    } else {
      log('Reseting from collectionId');
      this.reset((function () {
        log('Finished reset from collectionId');
        this._lastSyncCollectionId = collectionId;
        this.storage.put('lastSyncCollectionId', collectionId);
      }).bind(this));
      return false;
    }
  },

  // Getter
  lastSyncTime: function () {
    return this._lastSyncTime || 0;
  },

  _setLastSyncTime: function (timestamp) {
    if (typeof timestamp != 'number') {
      throw 'Must _setLastSyncTime to number (not ' + timestamp + ')';
    }
    this._lastSyncTime = timestamp;
    // Note that we're just storing this for later, so we don't need to know
    // when it gets really saved:
    this.storage.put('lastSyncTime', this._lastSyncTime);
  },

  _setSyncPosition: function (position) {
    this._syncPosition = position;
    this.storage.put('syncPosition', position);
  },

  _setLastSyncPut: function (timestamp) {
    if (typeof timestamp != 'number') {
      throw 'Must _setLastSyncPut to number (not ' + timestamp + ')';
    }
    this._lastSyncPut = timestamp;
    // Note that we're just storing this for later, so we don't need to know
    // when it gets really saved:
    this.storage.put('lastSyncPut', this._lastSyncPut);
  },

  /* Does a full sync, both getting updates and putting any pending
     local changes.

     By default if the collection has been deleted on the server this
     will fail with callback({collection_deleted: true}), but if
     forcePut is true then it will continue despite that change
     (effectively recreating the collection and ignore the delete).
     Ideally you should confirm with the user before recreating a
     collection this way. */
  syncNow: function (callback, forcePut) {
    log('Starting syncNow');
    logGroup();
    this._getUpdates((function (error) {
      if (error && error.collection_deleted && (! forcePut)) {
        log('Terminating sync due to collection deleted');
        logGroupEnd();
        return callback && callback(error);
      }
      if (error && (! error.collection_deleted)) {
        log('getUpdates error/terminating', {error: error});
        logGroupEnd();
        this.sendStatus({error: 'sync_get', detail: error});
        return callback && callback(error);
      }
      this.sendStatus({status: 'sync_get'});
      if (error && error.collection_deleted) {
        log('Collection is deleted, but ignoring');
        this.sendStatus({error: 'sync_get_deleted', detail: error});
        // However, since it's been deleted we need to reset, and we
        // can then continue with the put
        this.reset((function () {
          this._putUpdates(function (error) {
            logGroupEnd();
            return callback && callback(error);
          });
        }).bind(this));
        return;
      }
      this._putUpdates((function (error) {
        log('finished syncNow', {error: error});
        logGroupEnd();
        return callback && callback(error);
      }).bind(this));
    }).bind(this));
  },


  /* Deletes the server-side collection, not affecting anything local
     The reason is stored on the server. */
  deleteCollection: function (reason, callback) {
    if (typeof reason == 'string') {
      reason = {reason: reason};
    }
    if (! reason.client_id) {
      reason.client_id = 'unknown';
    }
    // FIXME: add client_id to reason (when we have a client_id)
    this.server.deleteCollection(reason, (function (error, result) {
      if (error) {
        this.sendStatus({error: 'delete_collection', detail: error});
        return callback && callback(error);
      }
      this._setLastSyncTime(0);
      this._setLastSyncPut(0);
      this.sendStatus({status: 'delete_collection'});
      return callback && callback(error, result);
    }).bind(this));
  },

  /* Gets updates, immediately applying any changes to the repo.

     Calls callback with no arguments on success */
  _getUpdates: function (callback) {
    if (this._syncPosition === undefined) {
      return callback && callback({error: "storage hasn't updated syncPosition yet"});
    }
    this.server.get(this._syncPosition, (function (error, results) {
      // FIXME: check error
      log('Ran GET', {since: this.lastSyncTime(), results: results, error: error});
      if (error) {
        this.sendStatus({error: 'server_get', detail: error});
        return callback && callback(error);
      }
      if (! this.confirmCollectionId(results.collection_id)) {
        // FIXME: should accept the new results
        this.reset();
        return callback && callback({error: "collection_id_changed"});
      }
      if (error) {
        return callback && callback(error);
      }
      this._processUpdates(results, callback);
    }).bind(this));
  },

  _processUpdates: function (results, callback) {
    if (results.objects.length) {
      this._setSyncPosition(results.objects[results.objects.length-1][0]);
      // FIXME: Do we care about the callback?
      var received = [];
      for (var i=0; i<results.objects.length; i++) {
        received.push(results.objects[i][1]);
      }
      this.appData.objectsReceived(received);
      this._setLastSyncTime(Date.now());
      if (results.incomplete) {
        log('Refetching next batch');
        this._getUpdates(callback);
        return;
      }
    } else {
      this._setLastSyncTime(Date.now());
    }
    return callback && callback();
  },

  /* Sends any updates the service finds to the remote server.

     Calls callback() with no arguments on success. */
  _putUpdates: function (callback) {
    this.appData.getPendingObjects((function (error, result) {
      if (error) {
        return callback && callback(error);
      }
      this._putUpdatedObjects(result, callback);
    }).bind(this));
  },

  _validateObjects: function(objects) {
    var errors = [];
    var allowedProps = ['type', 'id', 'expires', 'data', 'deleted'];
    for (var i=0; i<objects.length; i++) {
      var object = objects[i];
      if (! object.id) {
        errors.push({object: object, error: 'No .id property'});
      }
      if ('deleted' in object && object.deleted !== true) {
        errors.push({object: object, error: 'deleted property may only be true, or else shoudl be uset'});
      }
      if (object.deleted) {
        if ('data' in object) {
          errors.push({object: object, error: 'A deleted object cannot contain a .data property'});
        }
      }
      for (var prop in object) {
        if (! object.hasOwnProperty(prop)) {
          continue;
        }
        if (allowedProps.indexOf(prop) == -1) {
          errors.push({object: object, error: 'Object has property that is not allowed: ' + prop});
        }
      }
    }
    if (! errors.length) {
      errors = null;
    }
    return errors;
  },

  _putUpdatedObjects: function (objects, callback) {
    if (! objects.length) {
      log('No updates to send');
      return callback && callback();
    }
    var errors = this._validateObjects(objects);
    if (errors) {
      this.appData.reportObjectErrors(errors);
      return callback && callback({error: 'Objects have errors', detail: errors});
    }
    this.sendStatus({status: 'sync_put', count: objects.length});
    log('putUpdates', {updates: objects});
    // FIXME: we *must* include a 'since' key here to protect from
    // a concurrent update since our last get
    this.server.put(this._syncPosition, objects, (function (error, result) {
      log('server put completed', {error: error, result: result});
      if (error) {
        this.sendStatus({error: 'sync_put', detail: error});
        return callback && callback(error);
      }
      if (! this.confirmCollectionId(result.collection_id)) {
        return callback && callback({error: "collection_id_changed"});
      }
      if (result.since_invalid) {
        // the put failed and we need to process the updates and try again
        this.sendStatus({status: 'sync_put_precondition_failed'});
        this._processUpdates(result, (function (error) {
          if (error) {
            return callback && callback(error);
          }
          this._putUpdates(callback);
        }).bind(this));
        return;
      }
      this.sendStatus({status: 'sync_put_complete'});
      if ((! result.object_counters) || (! result.object_counters.length)) {
        // This shouldn't happen
        return callback && callback({error: 'No .object_counters received from server', result: result});
      }
      this._setSyncPosition(result.object_counters[result.object_counters.length-1]);
      // FIXME: do I care about the result of objectsSaved?
      this.appData.objectsSaved(objects);
      var now = Date.now();
      this._setLastSyncPut(now);
      this._setLastSyncTime(now);
      return callback && callback();
    }).bind(this));
  }

};



// Just logging helpers, should be removed at some later date...
function log(msg) {
  if (typeof console == 'undefined' || (! console.log)) {
    return;
  }
  var args = [msg];
  for (var i=1; i<arguments.length; i++) {
    var a = arguments[i];
    if (a === undefined || a === null || a === "") {
      continue;
    }
    if (typeof a == "object") {
      for (var j in a) {
        if (a.hasOwnProperty(j) && a[j] !== undefined && a[j] !== null && a[j] !== "") {
          args.push(j + ":");
          args.push(a[j]);
        }
      }
    } else {
      args.push(a);
    }
  }
  console.log.apply(console, args);
}

function logGroup() {
  if (console && console.group) {
    console.group();
  }
}

function logGroupEnd() {
  if (console && console.groupEnd) {
    console.groupEnd();
  } else if (console && console.endGroup) {
    console.endGroup();
  }
}

function objectValues(o) {
  if (o.length) {
    // It's already an array
    return o;
  }
  var result = [];
  for (var i in o) {
    if (o.hasOwnProperty(i)) {
      result.push(o[i]);
    }
  }
  return result;
}


/* A wrapper around the server API.

   The url is the url of the /verify entry point to the server */
Sync.Server = function (url, authenticator) {
  if (url.search(/^https?:\/\//i) == -1) {
    if (url.substr(0, 1) != '/') {
      url = '/' + url;
    }
    url = Sync.baseUrl.replace(/\/+$/, '') + url;
  }
  this._url = url;
  this._authenticator = authenticator;
  this._loginStatus = null;
  // This is a header sent with all requests (after login):
  this.authData = null;
  /* This is a callback for anytime a Retry-After or X-Sync-Poll-Time
     header is set, or when there is a 5xx error (all cases when the
     client should back off)

     Gets as its argument an object with .retryAfter (if that or
     X-Sync-Poll-Time is set), a value in seconds, and a .status
     attribute (integer response code).
  */
  this.onretryafter = null;
  /* This is a callback whenever there is a 401 error */
  this.onautherror = null;
};

Sync.Server.prototype = {

  /* Checks a request for Retry-After or X-Sync-Poll headers, and calls
     onretryafter.  This should be called for every request, including
     unsuccessful requests */
  checkRetryRequest: function (req) {
    if (! this.onretryafter) {
      // No one cares, so we don't need to check anything
      return;
    }
    var retryAfter = req.getResponseHeader('Retry-After');
    if (! retryAfter) {
      retryAfter = req.getResponseHeader('X-Sync-Poll-Time');
    }
    if (retryAfter) {
      var val = parseInt(retryAfter, 10);
      if (isNaN(val)) {
        // Might be a date...
        val = new Date(retryAfter);
        // Convert to seconds:
        val = parseInt((val - new Date()) / 1000, 10);
      }
      // Now some sanity checks:
      if (this.isSaneRetryAfter(val)) {
        this.onretryafter({retryAfter: val, status: req.status});
        return;
      }
    }
    if (req.status === 0 || (req.status >= 500 && req.status < 600)) {
      this.onretryafter({status: req.status});
    }
  },

  isSaneRetryAfter: function (val) {
    if (isNaN(val) || ! val) {
      return false;
    }
    if (val <= 0) {
      return false;
    }
    if (val > 60*60*24*2) {
      // Any value over 2 days is too long
      return false;
    }
    return true;
  },

  checkAuthRequest: function (req) {
    if (req.status === 401 && this.onautherror) {
      this.logout();
      this.onautherror();
    }
  },

  checkRequest: function (req) {
    this.checkRetryRequest(req);
    this.checkAuthRequest(req);
  },

  XMLHttpRequest: XMLHttpRequest,

  _createRequest: function (method, url) {
    var req;
    req = new this.XMLHttpRequest();

    req.open(method, url);
    this._authenticator.modifyRequest(req);
    return req;
  },

  /* Does a GET request on the server, getting all updates since the
     given timestamp */
  get: function (since, callback) {
    if (since === null) {
      since = 0;
    }
    if (! this._authenticator.loggedIn()) {
      throw 'You have not yet logged in';
    }
    if (typeof since != 'number' || isNaN(since)) {
      console.trace();
      throw 'In get(since, ...) since must be a number or null, not: ' + since;
    }
    var url = this._url;
    url += '?since=' + encodeURIComponent(since);
    var req = this._createRequest('GET', url);
    req.onreadystatechange = (function () {
      if (req.readyState != 4) {
        return;
      }
      this.checkRequest(req);
      if (req.status != 200) {
        callback({error: "Non-200 response code", code: req.status, url: url, request: req, text: req.responseText});
        return;
      }
      var data;
      try {
        data = JSON.parse(req.responseText);
      } catch (e) {
        callback({error: "invalid_json", exception: e, data: req.responseText});
        return;
      }
      if (data.collection_deleted) {
        callback(data, null);
      } else {
        callback(null, data);
      }
    }).bind(this);
    req.send();
  },

  // FIXME: should have since/lastget here, to protect against concurrent puts
  put: function (since, data, callback) {
    if (! this._authenticator.loggedIn()) {
      throw 'You have not yet logged in';
    }
    data = JSON.stringify(data);
    since = since || 0;
    if (typeof since != "number") {
      throw "put(since, ...): since must be a number (not " + since + ")";
    }
    var url = this._url + '?since=' + encodeURIComponent(since);
    var req = this._createRequest('POST', url);
    // FIXME: add since?
    req.onreadystatechange = (function () {
      if (req.readyState != 4) {
        return;
      }
      this.checkRequest(req);
      if (req.status != 200) {
        return callback && callback({error: "Non-200 response code", code: req.status, url: url, request: req});
      }
      var data = JSON.parse(req.responseText);
      callback(null, data);
    }).bind(this);
    req.send(data);
  },

  deleteCollection: function (reason, callback) {
    if (! this._authenticator.loggedIn()) {
      throw 'You have not logged in yet';
    }
    var data = JSON.stringify(reason);
    var url = this._url + '?delete';
    var req = this._createRequest('POST', url);
    req.onreadystatechange = function () {
      if (req.readyState != 4) {
        return;
      }
      // We don't call checkRequest() because we don't have retry-after handling for this
      // operation:
      this.checkAuthRequest(req);
      if (req.status != 200) {
        callback({error: "Non-200 response code", code: req.status, url: url, request: req});
        return;
      }
      var data;
      if (req.responseText) {
        data = JSON.parse(req.responseText);
      } else {
        data = null;
      }
      callback(null, data);
    };
    req.send(data);
  },

  toString: function () {
    return '[Sync.Server url: ' + this._url + ']';
  }

};


Sync.Scheduler = function (service) {
  this.service = service;
  this.service.onlogin = function () {
    self.activate();
  };
  this.service.onlogout = function () {
    self.deactivate();
  };
  this._timeoutId = null;
  this._period = this.settings.normalPeriod;
  // This is an amount to be added to the *next* request period,
  // but not repeated after:
  this._periodAddition = 0;
  this._retryAfter = null;
  if (this.service.loggedIn()) {
    this.activate();
  }
  this.lastSuccessfulSync = null;
  this.onerror = null;
  this.onsuccess = null;
  this.service.server.onretryafter = function (value) {
    self.retryAfter(value);
    self.schedule();
  };
};

Sync.Scheduler.prototype = {

  /* These default settings inform some of the adaptive scheduling */
  // FIXME: do some adaptive scheduling
  settings: {
    // This is as long as we allow successive backoffs to get:
    maxPeriod: 60*60000, // 1 hour
    // This is as short as we allow the time to get:
    minPeriod: 30 * 1000, // 30 seconds
    // Each time there's a failure (e.g., 5xx) we increase the period by this much:
    failureIncrease: 5*60000, // +5 minute for each failure
    // This is how often we poll normally:
    normalPeriod: 5*60000, // 5 minutes
    // When the repo is updated we sync within this amount of time
    // (this allows quick successive updates to be batched):
    immediateUpdateDelay: 500 // .5 seconds
  },

  /* Called when we should start regularly syncing (generally after
     login).  We also do one sync *right now* */
  activate: function () {
    this.deactivate();
    this.resetSchedule();
    // This forces the next sync to happen immediately:
    this._periodAddition = -this._period;
    this.schedule();
  },

  /* Stops any regular syncing, if any is happening */
  deactivate: function () {
    if (this._timeoutId) {
      clearTimeout(this._timeoutId);
      this._timeoutId = null;
    }
  },

  /* Resets the schedule to the normal pacing, undoing any adjustments */
  resetSchedule: function () {
    this._period = this.settings.normalPeriod;
    this._periodAddition = 0;
  },

  /* Schedules the next sync job, using this._period and this._periodAddition */
  schedule: function () {
    if (this._timeoutId) {
      clearTimeout(this._timeoutId);
    }
    this._timeoutId = setTimeout((function () {
      try {
        this.service.syncNow(function (error, result) {
          if (error && this.onerror) {
            this.onerror(error);
          }
          if (! error) {
            // Reset period on success:
            this.resetSchedule();
          }
          this.schedule();
          this.lastSuccessfulSync = Date.now();
          if (this.onsuccess) {
            this.onsuccess();
          }
        });
      } catch (e) {
        if (this.onerror) {
          this.onerror(e);
        }
        this.schedule();
      }
    }).bind(this), this._period + this._periodAddition);
    this._periodAddition = 0;
  },

  /* Run sync immediately, or at least very soon */
  scheduleImmediately: function () {
    this._periodAddition = (-this._period) + this.settings.immediateUpdateDelay;
    this.schedule();
  },

  /* Use this to do very few sync operations, typically when the user wouldn't
     care about promptness (e.g., dashboard is not visible) */
  scheduleSlowly: function () {
    this._period = this.settings.maxPeriod;
    this.schedule();
  },

  /* Called when the server gives back a Retry-After or similar response
     that should affect scheduling */
  retryAfter: function (value) {
    var retryAfter = value.retryAfter;
    if (! retryAfter) {
      // Must be a 5xx error
      this._period += this.settings.failureIncrease;
      if (this._period > this.settings.maxPeriod) {
        this._period = this.settings.maxPeriod;
      }
    } else {
      retryAfter = retryAfter * 1000;
      if (retryAfter < this.settings.minPeriod) {
        retryAfter = this.settings.minPeriod;
      } else if (retryAfter > this.settings.maxPeriod) {
        // FIXME: should I have a second higher level than maxPeriod here?
        retryAfter = this.settings.maxPeriod;
      }
      this._periodAddition = retryAfter - this._period;
    }
  }

};

Sync.LocalStorage = function (prefix) {
  if (this === window) {
    throw 'You forgot new Sync.LocalStorage';
  }
  this._prefix = prefix || '';
};

Sync.LocalStorage.prototype = {
  toString: function () {
    return '[Sync.LocalStorage prefix: ' + this._prefix + ']';
  },

  get: function (attributes, callback) {
    var isList = true;
    if (typeof attributes == "string") {
      attributes = [attributes];
      isList = false;
    }
    var result = {};
    for (var i=0; i<attributes.length; i++) {
      var name = attributes[i];
      var value = localStorage.getItem(this._prefix + name);
      if (value) {
        // FIXME: catch errors?
        value = JSON.parse(value);
      }
      result[name] = value;
    }
    if (! isList) {
      result = result[attributes[0]];
    }
    callback(result);
  },

  put: function (attributes, valueOrCallback, callback) {
    if (typeof attributes == "string") {
      var name = attributes;
      attributes = {};
      attributes[name] = valueOrCallback;
    } else {
      callback = valueOrCallback;
    }
    for (var i in attributes) {
      if (attributes.hasOwnProperty(i)) {
        var value = attributes[i];
        if (value === null) {
          localStorage.removeItem(this._prefix + i);
        } else {
          value = JSON.stringify(value);
          localStorage.setItem(this._prefix + i, value);
        }
      }
    }
    if (callback) callback();
  },

  clear: function (callback) {
    if (! this._prefix) {
      return callback && callback({error: 'Cannot clear without a prefix'});
    }
    var toDelete = [];
    for (var i=0; i<localStorage.length; i++) {
      var key = localStorage.key(i);
      if (key.indexOf(this._prefix) === 0) {
        toDelete.push(key);
      }
    }
    for (var i=0; i<toDelete.length; i++) {
      localStorage.removeItem(toDelete[i]);
    }
    return callback && callback();
  }

};