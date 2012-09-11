window.indexedDB = window.indexedDB || window.webkitIndexedDB || window.mozIndexedDB || window.msIndexedDB;
window.IDBTransaction = window.IDBTransaction || window.webkitIDBTransaction || window.OIDBTransaction || window.msIDBTransaction;
window.IDBKeyRange = window.IDBKeyRange || window.webkitIDBKeyRange;

var fileTemplate;

$(function () {

  $('#share-drop').on('dragover', function (event) {
    // FIXME: highlight something here?
    return false;
  });

  // jQuery doesn't bind this right with .on('drop'):
  $('#share-drop')[0].addEventListener('drop', function (event) {
    if ((! event.dataTransfer) || (! event.dataTransfer.files)) {
      console.log("Drop event wasn't for a file", event);
      return;
    }
    event.stopPropagation();
    event.preventDefault();
    var files = event.dataTransfer.files;
    var success = SuccessCollector(refreshList);
    for (var i=0; i<files.length; i++) {
      var file = new File(files[i]);
      AllFiles.putFile(file, success.onsuccess());
    }
  }, false);

  $('#file-upload').on('change', function () {
    var files = this.files;
    var success = SuccessCollector(refreshList);
    for (var i=0; i<files.length; i++) {
      var file = new File(files[i]);
      AllFiles.putFile(file, success.onsuccess());
    }
    $(this).val('');
  });

  fileTemplate = _.template($('#file-template').text());

  $(document).on("click", "button.remove", function (event) {
    var li = $(this).closest("li");
    var name = li.find(".filename").text();
    AllFiles.removeFile(name, refreshList);
    li.remove();
  });

  $('#login').click(function () {
    sync.toggleLogin();
    return false;
  });

  _domReady();

});

function Files(options) {
  this.onerror = options.onerror || this.defaultOnerror;
  this.onready = options.onready;
  var db = options.db || this.DB_NAME;
  if (typeof db == "string") {
    this._openDb(db);
  } else {
    this.db = db;
  }
}

Files.prototype = {
  DB_VERSION: 26,
  DB_NAME: "Files",

  defaultOnerror: function () {
    var args = ['DB Error:'];
    for (var i=0; i<arguments.length; i++) {
      args.push(arguments[i]);
    }
    console.error.apply(console, args);
  },

  _openDb: function (name) {
    name = name || 'Files';
    var req = indexedDB.open(name, this.DB_VERSION);
    greq = req;
    this.run({
      request: req,
      context: "Opening database",
      onsuccess: function (db) {
        if (db.setVersion && db.version != this.DB_VERSION) {
          console.warn('Upgrading database with setVersion');
          this.run({
            request: db.setVersion(this.DB_VERSION),
            context: "setVersion / database upgrade",
            onsuccess: function (result, request) {
              this.createDBStores(request.transaction, db);
            }
          });
        } else {
          console.log('Database opened normally');
          this.db = db;
          this.onready && this.onready();
        }
      },
      onupgradeneeded: function (db, request) {
        console.warn('Upgrading database');
        this.createDBStores(request.transaction, db);
      }
    });
  },

  createDBStores: function (transaction, db) {
    this.createStore(db, transaction, "files", {keyPath: "name"}, function (files) {
      /* This doesn't really seem necessary:
      console.log('Checking existing objects');
      this.run({
        request: files.openCursor(),
        context: "createDBStores open cursor",
        onsuccess: function (cursor) {
          if (cursor) {
            console.log('Removing bad object', cursor.value);
            if (! cursor.value.name) {
              files.delete(cursor.key);
            }
            cursor.continue();
          }
        }
      });
      */
    }, function (files) {
      files.createIndex("name", "name", {unique: true});
    }, function (files) {
      files.createIndex("saved", "saved", {unique: false});
    });
    this.createStore(db, transaction, "deletedFiles", {keyPath: "name"}, function (deletedFiles) {
      deletedFiles.createIndex("name", "name", {unique: true});
    });
    this.db = db;
    transaction.oncomplete = (function () {
      this.onready && this.onready();
    }).bind(this);
  },

  createStore: function (db, transaction, name, options) {
    var store = null;
    var after = [];
    for (var i=4; i<arguments.length; i++) {
      after.push(arguments[i]);
    }
    try {
      store = db.createObjectStore(name, options);
    } catch (e) {
      if (e instanceof DOMException && e.name == 'ConstraintError') {
        store = transaction.objectStore(name);
      } else {
        throw e;
      }
    }
    if (after.length) {
      for (i=0; i<after.length; i++) {
        try {
          after[i].call(this, store);
        } catch (e) {
          if (! (e instanceof DOMException && e.name == 'ConstraintError')) {
            throw e;
          }
        }
      }
    }
  },

  run: function (options) {
    var domRequest = options.request;
    var self = options.self || this;
    // FIXME: maybe I should just check for any on* methods?
    var methodNames = ["onsuccess", "onupgradeneeded", "onload"];
    methodNames.forEach(function (methodName) {
      var callback = options[methodName];
      if (callback) {
        domRequest[methodName] = function () {
          try {
            callback.call(self, this.result, this);
          } catch (e) {
            console.warn("Error in " + methodName + "() context", options.context);
            throw e;
          }
        };
      }
    });
    domRequest.onerror = function () {
      if (options.onerror) {
        options.onerror.call(self, this.error);
      }
      self.onerror(options.context || 'unknown context:', this.error);
    };
  },

  getStores: function () {
    var mode = "readonly";
    var args = [];
    var stores = {};
    for (var i=0; i<arguments.length; i++) {
      var arg = arguments[i];
      if (["readwrite", "readonly"].indexOf(arg) == -1) {
        args.push(arg);
      } else {
        mode = arg;
      }
    }
    if (! args.length) {
      throw 'You must provide at least one store to open';
    }
    stores.transaction = this.db.transaction(args, mode);
    for (var i=0; i<args.length; i++) {
      stores[args[i]] = stores.transaction.objectStore(args[i]);
    }
    return stores;
  },

  iterFiles: function (callback) {
    var files = this.getStores("files").files;
    this.run({
      request: files.openCursor(),
      context: "iterFiles",
      onsuccess: function iterFiles_createFile(cursor) {
        if (cursor) {
          var file = new File(null, cursor.value);
          callback(file);
          cursor.continue();
        } else {
          callback();
        }
      }
    });
  },

  getAllUnsaved: function (callback) {
    var result = [];
    var stores = this.getStores("files", "deletedFiles");
    var success = SuccessCollector(function () {
      callback(result);
    });
    var filesDone = success.onsuccess();
    var deletedFilesDone = success.onsuccess();
    this.run({
      request: stores.files.index("saved").openCursor(0),
      context: "getAllUnsaved files",
      onsuccess: function (cursor) {
        if (cursor) {
          var file = new File(null, cursor.value);
          result.push(file);
          cursor.continue();
        } else {
          filesDone();
        }
      }
    });
    this.run({
      request: stores.deletedFiles.openCursor(),
      context: "getAllUnsaved deletedFiles",
      onsuccess: function (cursor) {
        if (cursor) {
          var deletedFile = new DeletedFile(cursor.key);
          result.push(deletedFile);
          cursor.continue();
        } else {
          deletedFilesDone();
        }
      }
    });
  },

  getAllFiles: function (callback) {
    var result = [];
    this.iterFiles(function getAllFiles(file) {
      if (file !== undefined) {
        result.push(file);
      } else {
        callback(result);
      }
    });
  },

  iterDeleted: function (callback) {
    var deletedFiles = this.getStores("deletedFiles").deletedFiles;
    this.run({
      request: deletedFiles.openCursor(),
      context: "iterDeleted",
      onsuccess: function (cursor) {
        if (cursor) {
          callback(new DeletedFile(cursor.key));
          cursor.continue();
        } else {
          callback();
        }
      }
    });
  },

  getAllDeleted: function (callback) {
    var result = [];
    this.iterDeleted(function (file) {
      if (file !== undefined) {
        result.push(file);
      } else {
        callback(result);
      }
    });
  },

  putFile: function (file, callback) {
    if (file.fileObject && ! file.blob) {
      file.readFileObject((function () {
        this.putFile(file, callback);
      }).bind(this));
      return;
    }
    var saved = file.saved ? 1 : 0;
    var fileData = {
      name: file.name,
      type: file.type,
      size: file.size,
      saved: saved,
      blob: file.blob,
      href: file.href
    };
    var stores = this.getStores("readwrite", "files", "deletedFiles");
    var files = stores.files;
    var deletedFiles = stores.deletedFiles;
    this.run({
      request: deletedFiles.delete(fileData.name),
      context: "putFile remove deletedFiles"
    });
    console.log('saved file', fileData);
    this.run({
      request: files.put(fileData),
      context: "putFile",
      onsuccess: function () {
        if (! file.saved) {
          sync.scheduleImmediately();
        }
        callback && callback();
      }
    });
  },

  removeFile: function (name, callback) {
    if (name.name) {
      name = name.name;
    }
    var stores = this.getStores("readwrite", "files", "deletedFiles");
    var success = new SuccessCollector(function () {
      callback();
      sync.scheduleImmediately();
    });
    this.run({
      request: stores.files.delete(name),
      context: "removeFile from files",
      onsuccess: success.onsuccess()
    });
    this.run({
      request: stores.deletedFiles.put({name: name}),
      context: "removeFiles add to deletedFiles",
      onsuccess: success.onsuccess()
    });
  },

  removeFully: function (name, callback) {
    var stores = this.getStores("readwrite", "files", "deletedFiles");
    var success = new SuccessCollector(callback);
    this.run({
      request: stores.files.delete(name),
      context: "removeFully from files",
      onsuccess: success.onsuccess()
    });
    this.run({
      request: stores.deletedFiles.delete(name),
      context: "removeFully from deletedFiles",
      onsuccess: success.onsuccess()
    });
  }

};

function File(fileObj, data) {
  data = data || {};
  if (fileObj) {
    this.fileObject = fileObj;
    data.name = data.name || fileObj.name;
    data.type = data.type || fileObj.type;
    data.size = data.size || fileObj.size;
  }
  this.name = data.name;
  if (! this.name) {
    console.trace();
    throw 'No name provided or in data';
  }
  this.type = data.type;
  this.size = data.size;
  this.saved = data.saved || false;
  this.blob = data.blob;
  this.href = data.href;
}

File.fromSync = function (syncData, saved) {
  // FIXME: could saved ever be false?
  return new this(null, {
    name: syncData.id,
    type: syncData.blob.content_type,
    size: syncData.data.size,
    href: syncData.blob.href,
    saved: saved
  });
};

File.prototype = {
  readFileObject: function (callback) {
    var reader = new FileReader();
    var self = this;
    reader.onload = function () {
      self.blob = this.result;
      callback && callback();
    };
    reader.onerror = function () {
      console.error("Error reading file:", this.error);
      self.blob = null;
      callback && callback(this.error);
    };
    reader.readAsArrayBuffer(this.fileObject);
  },

  formattedSize: function () {
    if (this.size > 1000000) {
      return parseInt(this.size / 1000000, 10) + ' Mb';
    } else if (this.size > 1000) {
      return parseInt(this.size / 1000, 10) + ' Kb';
    } else {
      return this.size + ' bytes';
    }
  },

  downloadLink: function (forceDownload) {
    var url = this.href;
    if (! url) {
      return null;
    }
    if (sync) {
      url = sync.authenticateUrl(url);
    }
    if (forceDownload) {
      if (url.indexOf('?') == -1) {
        url += '?';
      } else {
        url += '&';
      }
      url += 'filename=' + encodeURIComponent(this.name);
    }
    return url;
  },

  syncRepr: function () {
    var data = {
      type: "file",
      id: this.name,
      data: {
        size: this.size
      },
      blob: {
        content_type: this.type
      }
    };
    if (this.href) {
      data.blob.href = this.href;
    } else {
      data.blob.data = blobToBase64(this.blob);
    }
    return data;
  }

};

function DeletedFile(name) {
  this.name = name;
}

DeletedFile.prototype = {
  syncRepr: function () {
    return {type: "file", id: this.name, deleted: true};
  }
};

function SuccessCollector(ondone) {
  if (this === window) {
    return new SuccessCollector(ondone);
  }
  this.ondone = ondone;
  this.counts = 0;
  this.args = [];
}

SuccessCollector.prototype = {
  onsuccess: function () {
    var count = this.counts;
    this.counts++;
    this.args.push(null);
    var self = this;
    return function () {
      var result = this.result || null;
      if ((! result) && arguments.length) {
        result = [];
        for (var i=0; i<arguments.length; i++) {
          result.push(arguments[i]);
        }
      }
      self.args[count] = result;
      self.counts--;
      if ((! self.counts) && self.ondone) {
        var ondone = self.ondone;
        ondone(self.args);
      }
    };
  }
};

function refreshList() {
  AllFiles.getAllFiles(function (files) {
    var container = $('#share-list');
    container.html('');
    files.forEach(function (file) {
      var result = fileTemplate({
        file: file
      });
      container.append($(result));
    });
    if (! files.length) {
      container.append($('<li id="null-share">None yet!</li>'));
    }
  });
}

var readySuccess = SuccessCollector(function () {
  setupSync();
  refreshList();
});
var _domReady = readySuccess.onsuccess();

var AppData = {

  getPendingObjects: function (callback) {
    var objects = [];
    AllFiles.getAllUnsaved(function (files) {
      var result = [];
      for (var i=0; i<files.length; i++) {
        result.push(files[i].syncRepr());
      }
      callback(null, result);
    });
  },

  objectsSaved: function (objects) {
    console.log('Objects saved:', objects);
    var success = SuccessCollector(refreshList);
    objects.forEach(function (o) {
      if (o.deleted) {
        // Unlike additions, I don't care about these removals or when
        // they finish, because they are removing obsolete objects:
        var req = AllFiles.removeFully(o.id);
      } else {
        var file = File.fromSync(o, true);
        AllFiles.putFile(file, success.onsuccess());
      }
    });
  },

  objectsReceived: function (objects) {
    console.log('Objects received:', objects);
    var success = SuccessCollector(refreshList);
    objects.forEach(function (o) {
      if (o.deleted) {
        AllFiles.removeFully(o.id, success.onsuccess());
      } else {
        var file = File.fromSync(o, true);
        AllFiles.putFile(file, success.onsuccess());
      }
    });
  }

};

var AllFiles = new Files({
  onready: readySuccess.onsuccess()
});

var sync;

function setupSync() {
  sync = new Sync(AppData, {appName: 'files'});
  // FIXME: technically since these touch the DOM should we wait until the DOM is ready?
  sync.watch({
    onlogin: function (email) {
      $('#login').text(email);
      $('#login').attr('title', 'Click to log out');
    },
    onlogout: function () {
      $('#login').text('login');
      $('#login').attr('title', 'Click to log in');
    }
  });
}

function blobToBase64(blob) {
  // Oh this is just terrible
  var binary = '';
  var bytes = new Uint8Array(blob);
  var len = bytes.byteLength;
  for (var i=0; i<len; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
