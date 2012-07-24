var Authenticator = function (user, domain) {

  if (! user) {
    user = "test-"+(Date.now())+"@example.com";
  }

  if (! domain) {
    domain = location.hostname;
  }

  return {
    modifyRequest: function (req) {
      req.setRequestHeader('X-Remote-User', user + '/' + domain);
    },

    modifyUrl: function (url) {
      return url;
    },

    loggedIn: function () {
      return true;
    },

    logout: function () {
      throw 'Not implemented';
    },

    request: function () {
      throw 'Not implemented';
    },

    email: user,

    domain: domain,

    serverUrl: (doctest.params.server || '/') +
      encodeURIComponent(domain) +
      '/' + encodeURIComponent(user) + "/bucket",

    watch: function (options) {
      // FIXME: do something with options
    }
  };
};

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
