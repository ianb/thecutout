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
    for (var i=0; i<files.length; i++) {
      saveFile(files[i]);
      shareFile(files[i]);
    }
  }, false);

  $('#file-upload').on('change', function () {
    var files = this.files;
    for (var i=0; i<files.length; i++) {
      saveFile(files[i]);
      shareFile(files[i]);
    }
    $(this).val('');
  });

  fileTemplate = _.template($('#file-template').text());

  $(document).on("click", "button.remove", function (event) {
    var li = $(this).closest("li");
    var name = li.find(".filename").text();
    removeFile(name);
    li.remove();
  });

  $('#login').click(function () {
    if ($('#login').text() == 'login') {
      sync.request();
    } else {
      sync.logout();
    }
    return false;
  });

  ready("dom");

});

function reportError() {
  console.error("Error:", this.error);
}

var dbRequest = indexedDB.open("FilesToShare3");

var db = null;
dbRequest.onerror = reportError;
var DB_VERSION = 5;
dbRequest.onsuccess = function () {
  console.warn('Opening', this.result, dbRequest.result, arguments[0]);
  db = this.result;
  console.log('Opened database', db, db.version);
  if (db.setVersion && db.version != DB_VERSION) {
    createDBStoresSetVersion();
  } else {
    ready("db");
  }
};
dbRequest.onupgradeneeded = function () {
  db = this.result;
  createDBStores(this.transaction);
};
dbRequest.onerror = function () {
  console.error('Database failed to open', dbRequest.error);
};

function createDBStoresSetVersion() {
  var versionReq = db.setVersion(DB_VERSION);
  versionReq.onerror = reportError;
  versionReq.onsuccess = createDBStores;
}

function createDBStores(transaction) {
  console.log('Set new version');
  var fileStore = db.createObjectStore("files", {keyPath: "filename"});
  fileStore.createIndex("filename", "filename", {unique: true});
  fileStore.createIndex("saved", "saved", {unique: false});
  var deletedStore = db.createObjectStore("deletedFiles", {keyPath: "filename"});
  deletedStore.createIndex("filename", "filename", {unique: true});
  if (transaction) {
    transaction.oncomplete = function () {
      console.log('Created stores');
      ready("db");
    };
  }
}

function ready(name) {
  ready[name] = true;
  if (ready.db && ready.dom) {
    $('#share-list').html('');
    iterFiles(function (err, key, value) {
      if (value) {
        shareFile(value);
      }
    });
  }
}

function shareFile(file) {
  var result = fileTemplate({filename: file.name || file.filename, type: file.type, size: file.size, href: file.href});
  result = $(result);
  $('#null-share').remove();
  $('#share-list').append(result);
}

function iterFiles(callback) {
  var fileStore = db.transaction(["files"]).objectStore("files");
  fileStore.openCursor().onsuccess = function () {
    var cursor = this.result;
    if (cursor) {
      callback(null, cursor.key, cursor.value);
      cursor.continue();
    } else {
      callback();
    }
  };
}

function saveFile(fileObj, callback) {
  console.log('Saving file', fileObj);
  var reader = new FileReader();
  reader.onload = function () {
    var blob = this.result;
    console.log('read the bytes', blob, blob.byteLength);
    var trans = db.transaction(["files"], "readwrite");
    var fileData = {
      filename: fileObj.name,
      type: fileObj.type,
      size: fileObj.size,
      saved: 0,
      content: blob
    };
    var store = trans.objectStore("files");
    console.log('Inserting', fileData);
    var ins = store.put(fileData);
    ins.onsuccess = function () {
      if (callback) {
        callback();
      }
    };
    ins.onerror = function () {
      if (callback) {
        callback(this.error);
      }
    };
  };
  reader.onerror = function () {
    if (callback) {
      callback(this.error);
    }
  };
  reader.readAsArrayBuffer(fileObj);
}

function removeFile(name, callback) {
  console.log('Removing file', name);
  var trans = db.transaction(["files", "deletedFiles"], "readwrite");
  var store = trans.objectStore("files");
  var deletedFiles = trans.objectStore("deletedFiles");
  var req = store.delete(name);
  req.onsuccess = function () {
    var delReq = deletedFiles.put({filename: name});
    delReq.onsuccess = function () {
      if (callback) {
        callback();
      }
    };
    delReq.onerror = function () {
      if (callback) {
        callback(this.error);
      }
    };
  };
  req.onerror = function () {
    if (callback) {
      callback(this.error);
    }
  };
}

function formatSize(bytes) {
  if (bytes > 1000000) {
    return parseInt(bytes/1000000, 10) + ' Mb';
  } else if (bytes > 1000) {
    return parseInt(bytes/1000, 10) + ' Kb';
  } else {
    return bytes + ' bytes';
  }
}

var AppData = {

  getPendingObjects: function (callback) {
    console.log('asked for pending objects');
    var objects = [];
    var trans = db.transaction(["files", "deletedFiles"]);
    var fileStore = trans.objectStore("files");
    var deletedFiles = trans.objectStore("deletedFiles");
    var index = fileStore.index("saved");
    var req = index.openCursor(0);
    req.onerror = function () {
      console.error('pending object error', this.error);
    };
    req.onsuccess = function () {
      var cursor = req.result;
      if (cursor) {
        objects.push({
          type: "file",
          id: cursor.value.filename,
          data: {
            size: cursor.value.size
          },
          blob: {
            content_type: cursor.value.type,
            data: blobToBase64(cursor.value.content)
          }
        });
        cursor.continue();
        console.log('added item', objects[objects.length-1].id);
      } else {
        console.log('got all items; looking for deleted');
        var delReq = deletedFiles.openCursor();
        delReq.onsuccess = function () {
          var cursor = this.result;
          if (cursor) {
            objects.push({
              type: "file",
              id: cursor.value.filename,
              deleted: true
            });
            console.log('added deleted', objects[objects.length-1].id);
            cursor.continue();
          } else {
            console.log('pending:', objects.map(function (i) {return i.id;}));
            callback(null, objects);
          }
        };
        delReq.onerror = function () {
          console.error('pending object error', this.error);
        };
      }
    };
  },

  objectsSaved: function (objects) {
    var trans = db.transaction(["files", "deletedFiles"], "readwrite");
    var fileStore = trans.objectStore("files");
    var deletedFiles = trans.objectStore("deletedFiles");
    var index = fileStore.index("filename");
    console.log('Objects saved:', objects);
    objects.forEach(function (o) {
      if (o.deleted) {
        var req = deletedFiles.delete(o.id);
      } else {
        var req = index.get(IDBKeyRange.only(o.id));
        req.onsuccess = function () {
          var value = req.result;
          // FIXME: is there really any reason to check the value?
          if (! value) {
            console.log('Could not find corresponding value:', o);
          }
          var newValue = {
            saved: 1,
            content: null,
            filename: o.id,
            type: o.blob.content_type,
            href: o.blob.href,
            size: o.data.size
          };
          fileStore.put(newValue);
          // I don't care about the success callback on the put
        };
      }
    });
    ready("refresh");
  },

  objectsReceived: function (objects) {
    console.log('Objects received:', objects);
    var trans = db.transaction(["files", "deletedFiles"], "readwrite");
    var fileStore = trans.objectStore("files");
    var deletedFiles = trans.objectStore("deletedFiles");
    objects.forEach(function (o) {
      if (o.deleted) {
        fileStore.delete(o.id);
        deletedFiles.delete(o.id);
      } else {
        var newValue = {
          saved: 1,
          content: null,
          filename: o.id,
          type: o.blob.content_type,
          href: o.blob.href,
          size: o.data.size
        };
        fileStore.put(newValue);
      }
    });
    // FIXME: this isn't waiting for commit properly:
    setTimeout(function () {ready("refresh");}, 100);
  }

};

var sync;
sync = new Sync(AppData, {appName: 'files'});
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
