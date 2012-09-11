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

Sync.PersonaAuthenticator: handles authenticating with Persona/BrowserID

Also:

Sync.LocalStorage: storage implementation that uses localStorage, for
keeping sync-related persistent metadata.


All functions here use Node-style error handling, where the functions
take a callback with the signature callback(error, result), where
error is null or undefined in case of success (result itself is
optional depending on the function).

*/

function Sync(appData, options) {
  if (this === window) {
    throw "You forgot *new* Sync()";
  }
  if (! appData) {
    throw "You must provide a Sync(appData) appData argument";
  }
  Sync._allowOptions({
    name: 'Sync()',
    options: options,
    allowed: ['serverUrl', 'bucket', 'appName', 'verifyUrl', 'storage', 'assertion']
  })
  var serverUrl = options.serverUrl || '';
  var bucket = options.bucket;
  if (! bucket) {
    bucket = options.appName || 'bucket';
    bucket = '{domain}/{user}/' + bucket;
  }
  var verifyUrl = options.verifyUrl;
  if ((! serverUrl) && ! Sync.baseUrl) {
    throw 'syncclient.js has no hardcoded server URL; you must provide a serverUrl option';
  }
  if (! verifyUrl) {
    verifyUrl = Sync.baseUrl + serverUrl.replace(/\/+$/, '') + '/verify';
  }
  this.storage = options.storage || new Sync.LocalStorage('sync::');
  this.authenticator = new Sync.PersonaAuthenticator(verifyUrl, this.storage, options.assertion);
  this.server = new Sync.Server(serverUrl, bucket, this.authenticator);
  this.service = new Sync.Service(this.server, appData, this.storage);
  this.scheduler = new Sync.Scheduler(this.service, this.authenticator);
  if (appData.onupdate !== undefined) {
    appData.onupdate = this.scheduleImmediately.bind(this);
  }
};

Sync.prototype = {
  lastSyncTime: function () {
    return this.service.lastSyncTime();
  },
  resetSchedule: function () {
    this.scheduler.resetSchedule();
  },
  scheduleImmediately: function () {
    this.scheduler.scheduleImmediately();
  },
  scheduleSlowly: function () {
    this.scheduler.scheduleSlowly();
  },
  activate: function () {
    this.scheduler.activate();
  },
  deactivate: function () {
    this.scheduler.deactivate();
  },
  logout: function () {
    this.authenticator.logout();
  },
  request: function (args) {
    this.authenticator.request(args);
  },
  toggleLogin: function () {
    if (this.authenticator.loggedIn()) {
      this.authenticator.logout();
    } else {
      this.authenticator.request();
    }
  },
  watch: function (options) {
    this.authenticator.watch(options);
  },
  reset: function (callback) {
    this.service.reset(callback);
  },
  authenticateUrl: function (url) {
    return this.authenticator.modifyUrl(url);
  }
};

// Note, this URL gets rewritten by the server:
Sync.baseUrl = null;

Sync.noop = function () {};

Sync._allowOptions = function (args) {
  var ops = args.options;
  for (var i in ops) {
    if (ops.hasOwnProperty(i)) {
      if (args.allowed.indexOf(i) == -1) {
        throw 'The option "' + i + '" is now allowed for ' + args.name;
      }
    }
  }
};

Sync.Service = function (server, appData, storage) {
  if (this === window || this === Sync) {
    throw 'You forgot new Sync.Service';
  }
  this.server = server;
  this.appData = appData;
  if (! this.appData) {
    throw 'You must provide an appData argument';
  }
  this.storage = storage || new Sync.LocalStorage('sync::');
  this.storage.get(
    ['lastSyncTime', 'lastSyncPut', 'lastSyncCollectionId', 'syncPosition'],
    (function (values) {
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
    this.appData.resetSaved(Sync.noop);
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
    log('confirming collectionId', collectionId || 'no remote', this._syncPosition || 'no local');
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
        this.sendStatus({error: 'sync_get', detail: error});
        logGroupEnd();
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
        var err = null;
        if (error && error.length) {
          err = error.map(function (e) {
            if (e.error && e.object) {
              return e.error + ' for object id: ' + JSON.stringify(e.object.id);
            }
            return e;
          });
        }
        log('finished syncNow', {error: err});
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
      log('Ran GET', {since: this._syncPosition, results: results, error: error});
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
      var newPosition = results.until || results.objects[results.objects.length-1][0]
      var received = [];
      var seen = {};
      for (var i=results.objects.length-1; i>=0; i--) {
        var o = results.objects[i][1];
        var id = o.id + '||' + o.type;
        if (seen.hasOwnProperty(id)) {
          continue;
        }
        received.push(o);
        seen[id] = true;
      }
      received.reverse();
      var error = null;
      try {
        this.appData.objectsReceived(received, Sync.noop);
      } catch (e) {
        error = e;
      }
      if (error === null) {
        this._setSyncPosition(newPosition);
        this._setLastSyncTime(Date.now());
        if (results.incomplete) {
          log('Refetching next batch');
          this._getUpdates(callback);
          return;
        }
      }
      if (error) {
        return callback && callback(error);
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
      // FIXME: I *really wish* I could identify the difference
      // between "errors" and a mis-called callback(result).  Maybe
      // require error to be an object, not an array?
      if (error) {
        return callback && callback(error);
      }
      this._putUpdatedObjects(result, callback);
    }).bind(this));
  },

  _validateObjects: function(objects) {
    var errors = [];
    var allowedProps = ['type', 'id', 'expires', 'data', 'deleted', 'blob'];
    var checkDups = {};
    for (var i=0; i<objects.length; i++) {
      var object = objects[i];
      var objectId = (object.type || '') + '\000' + object.id;
      if (checkDups.hasOwnProperty(objectId)) {
        errors.push({
          object: object,
          error: 'Multiple objects have the type "' + (object.type || 'no type') +
                '" and the id "' + object.id + '"'
        });
      }
      checkDups[objectId] = true;
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
        if ('blob' in object) {
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

      if (object.blob) {
        for (var prop in object.blob) {
          if (! object.hasOwnProperty(prop)) {
            continue;
          }
          if (['href', 'data', 'content_type'].indexOf(prop) == -1) {
            errors.push({object: object, error: 'Object .blob has property that is not allowed: ' + prop});
          }
        }
        if ((! object.blob.href) && (! object.blob.data)) {
          errors.push({object: object, error: 'Object .blob must have a .blob.data or .blob.href property'});
        }
        if (! object.blob.content_type) {
          errors.push({object: object, error: 'Object .blob must have a .blob.content_type property'});
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
      if (this.appData.reportObjectErrors) {
        this.appData.reportObjectErrors(errors);
      } else {
        var err = errors.map(function (e) {
          if (e.error && e.object) {
            return e.error + ' for object id: ' + e.object.id;
          }
          return e;
        });
        log('Objects have errors:', err);
      }
      return callback && callback({error: 'Objects have errors', detail: errors});
    }
    this.sendStatus({status: 'sync_put', count: objects.length});
    log('putUpdates', {updates: objects});
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
      var error = null;
      if (result.blobs) {
        var objectsById = {};
        for (var i=0; i<objects.length; i++) {
          var o = objects[i];
          objectsById[(o.type || '') + '\000' + o.id] = o;
        }
        for (var i=0; i<result.blobs.length; i++) {
          var blob = result.blobs[i];
          var o = objectsById[(blob.type || '') + '\000' + blob.id];
          o.blob.href = blob.href;
        }
      }
      try {
        // FIXME: do I care about the result of objectsSaved?
        this.appData.objectsSaved(objects, Sync.noop);
      } catch (e) {
        // If there's an exception we'll consider the whole thing a bust
        error = e;
      }
      if (error === null) {
        var now = Date.now();
        this._setLastSyncPut(now);
        this._setLastSyncTime(now);
      }
      return callback && callback(error);
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
Sync.Server = function (baseUrl, bucketName, authenticator) {
  baseUrl = baseUrl || '';
  if (baseUrl.search(/^https?:\/\//i) == -1) {
    if (baseUrl && baseUrl.substr(0, 1) != '/') {
      baseUrl = '/' + baseUrl;
    }
    if (Sync.baseUrl) {
      baseUrl = Sync.baseUrl.replace(/\/+$/, '') + baseUrl;
    }
  }
  this._baseUrl = baseUrl;
  this._bucketName = bucketName.replace(/^\/+/, '').replace(/\/+$/, '');
  this._bucketUrl = null;
  this._authenticator = authenticator;
  this._authenticator.watch({
    onlogin: (function (email) {
      this.setVariables(authenticator.domain, email);
    }).bind(this),
    onlogout: (function () {
      this.setVariables(null, null);
    }).bind(this)
  });
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
    url = this._authenticator.modifyUrl(url);
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
    var url = this._bucketUrl;
    if (! url) {
      throw 'server.setVariables() has not yet been called';
    }
    url += '?since=' + encodeURIComponent(since);
    if (this._lastSyncCollectionId) {
      url += '&collection_id=' + encodeURIComponent(this._lastSyncCollectionId);
    }
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

  put: function (since, data, callback) {
    if (! this._authenticator.loggedIn()) {
      throw 'You have not yet logged in';
    }
    data = JSON.stringify(data);
    since = since || 0;
    if (typeof since != "number") {
      throw "put(since, ...): since must be a number (not " + since + ")";
    }
    var url = this._bucketUrl + '?since=' + encodeURIComponent(since);
    var req = this._createRequest('POST', url);
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
    var url = this._bucketUrl + '?delete';
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

  setVariables: function (domain, username) {
    // FIXME: I like nothing about this
    var name = this._bucketName;
    if (username) {
      name = name.replace(/\{user\}/g, encodeURIComponent(username));
    }
    if (domain) {
      name = name.replace(/\{domain\}/g, encodeURIComponent(domain));
    }
    this._bucketUrl = this._baseUrl + '/' + name;
  },

  toString: function () {
    return '[Sync.Server url: ' + (this._bucketUrl || this._baseUrl) + ']';
  }

};


Sync.Scheduler = function (service, authenticator) {
  this.service = service;
  this.authenticator = authenticator;
  this.authenticator.watch({
    onlogin: (function () {
      this.activate();
    }).bind(this),

    onlogout: (function () {
      this.deactivate();
    }).bind(this)
  });
  this._timeoutId = null;
  this._period = this.settings.normalPeriod;
  // This is an amount to be added to the *next* request period,
  // but not repeated after:
  this._periodAddition = 0;
  this._retryAfter = null;
  this.lastSuccessfulSync = null;
  this.onerror = null;
  this.onsuccess = null;
  this.service.server.onretryafter = (function (value) {
    this.retryAfter(value);
    this.schedule();
  }).bind(this);
  this.adjustForVisibility();
};

Sync.Scheduler.prototype = {

  /* These default settings inform some of the adaptive scheduling */
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
        this.service.syncNow((function (error, result) {
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
        }).bind(this));
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
  },

  /* Add a visibilitychange event, with browser compatibility.  Calls
     handler(event, hiddenState) when the state changes */
  _addVisibilityHandler: function (handler) {
    // Browser compatibility:
    var hidden, visibilityChange;
    if (typeof document.hidden !== "undefined") {
      hidden = "hidden";
      visibilityChange = "visibilitychange";
    } else if (typeof document.mozHidden !== "undefined") {
      hidden = "mozHidden";
      visibilityChange = "mozvisibilitychange";
    } else if (typeof document.msHidden !== "undefined") {
      hidden = "msHidden";
      visibilityChange = "msvisibilitychange";
    } else if (typeof document.webkitHidden !== "undefined") {
      hidden = "webkitHidden";
      visibilityChange = "webkitvisibilitychange";
    } else {
      // Otherwise the browser doesn't support the event
      return;
    }
    document.addEventListener(visibilityChange, function (event) {
      var state = document[hidden];
      handler.call(this, event, state);
    }, false);
  },

  adjustForVisibility: function () {
    this._addVisibilityHandler(this._visibilityChanged.bind(this));
  },

  _visibilityChanged: function (event, hidden) {
    if (hidden) {
      log('Scheduling slow due to tab being hidden');
      this.scheduleSlowly();
    } else {
      log('Scheduling immediately due to tab becoming visible');
      this._period = this.settings.normalPeriod;
      this.scheduleImmediately();
    }
  }

};

Sync.PersonaAuthenticator = function (verifyUrl, storage, assertion) {
  if (this === window || this === Sync) {
    throw 'You forgot new Sync.PersonaAuthenticator';
  }
  this.verifyUrl = verifyUrl;
  this.storage = storage || new Sync.LocalStorage('sync-auth::');
  this.authData = this.email = null;
  this.onlogins = [];
  this.onlogouts = [];
  this.domain = location.hostname;
  if (assertion) {
    this._assertionReceived(assertion);
  } else {
    this._loadFromStorage((function () {
      this._callWatch();
    }).bind(this));
  }
};

Sync.PersonaAuthenticator.prototype = {

  loggedIn: function () {
    return !!this.email;
  },

  _callOnlogin: function () {
    for (var i=0; i<this.onlogins.length; i++) {
      var callback = this.onlogins[i];
      callback(this.email, this.authData);
    }
  },

  _callOnlogout: function () {
    for (var i=0; i<this.onlogouts.length; i++) {
      var callback = this.onlogouts[i];
      callback();
    }
  },

  request: function (options) {
    navigator.id.request(options);
  },

  watch: function (options) {
    if (options.onlogin) {
      var onlogin = options.onlogin;
      this.onlogins.push(onlogin);
      if (this.authData) {
        onlogin(this.email, this.authData);
      }
    }
    if (options.onlogout) {
      var onlogout = options.onlogout;
      this.onlogouts.push(onlogout);
      if (! this.authData) {
        onlogout();
      }
    }
    if (options.verifyUrl) {
      this.verifyUrl = options.verifyUrl;
    }
  },

  logout: function () {
    navigator.id.logout();
    this.authData = this.email = null;
    this._callOnlogout();
  },

  _assertionReceived: function (assertion) {
    log('Received assertion', assertion.substr(0, 10)+'...');
    var req = new Sync.Server.prototype.XMLHttpRequest();
    req.open('POST', this.verifyUrl);
    var audience = location.protocol + '//' + location.host;
    log('POST', this.verifyUrl, 'audience:', audience);
    req.onreadystatechange = (function () {
      if (req.readyState != 4) {
        return;
      }
      if (req.status != 200) {
        log('Error in auth:', req.status, req.statusText);
        return;
      }
      try {
        var result = JSON.parse(req.responseText);
      } catch (e) {
        log('Error parsing response:', req.responseText);
        throw e;
      }
      if (result.status != "okay") {
        log('Login status not okay:', result);
        this._callOnlogout();
        return;
      }
      this.authData = result;
      this.email = this.authData.email;
      this.storage.put('authData', this.authData);
      this._callOnlogin();
    }.bind(this));
    req.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
    req.send('assertion=' + encodeURIComponent(assertion) +
             '&audience=' + encodeURIComponent(audience));
  },

  logout: function () {
    this.authData = this.email = null;
    this.storage.put('authData', null);
    this._callOnlogout();
  },

  _callWatch: function () {
    navigator.id.watch({
      loggedInEmail: this.email,
      onlogin: this._assertionReceived.bind(this),
      onlogout: this.logout.bind(this)
    });
  },

  _loadFromStorage: function (callback) {
    this.storage.get('authData', (function (data) {
      if (data) {
        this.authData = data;
        this.email = this.authData.email;
        this.authData.fromCache = true;
        this._callOnlogin();
      }
      if (callback) callback();
    }).bind(this));
  },

  modifyUrl: function (url) {
    if (! this.authData) {
      return url;
    }
    var auth = this.authData.auth;
    if (auth.query) {
      if (url.indexOf('?') == -1) {
        url += '?';
      } else {
        url += '&';
      }
      var q = auth.query;
      if (typeof q != "string") {
        var s = '';
        for (var key in q) {
          if (! q.hasOwnProperty(key)) {
            continue;
          }
          if (s) {
            s += '&';
          }
          s += encodeURIComponent(key) + '=' + encodeURIComponent(q[key]);
        }
        url += s;
      } else {
        url += q;
      }
    }
    return url;
  },

  modifyRequest: function (req) {
    if (! this.authData) {
      return;
    }
    var auth = this.authData.auth;
    if (auth.headers) {
      for (var header in auth.headers) {
        if (! auth.headers.hasOwnProperty(header)) {
          continue;
        }
        req.setRequestHeader(header, auth.headers[header]);
      }
    }
  }

};

Sync.LocalStorage = function (prefix) {
  if (this === window || this === Sync) {
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
