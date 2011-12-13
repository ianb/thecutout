import os
import shutil
import time
import simplejson as json
import tempfile


def find_to_remove(db, expire_time=None, start=0):
    """Finds objects that should be deleted, and returns a set of the
    counts of those objects"""
    seen = {}
    to_remove = set()
    if expire_time is None:
        expire_time = time.time()
    for count, item in db.read(start):
        parsed = json.loads(item)
        id = (parsed['id'], parsed.get('type'))
        if id in seen:
            to_remove.add(seen[id])
        if expire_time and parsed['expire'] and parsed['expire'] < expire_time:
            to_remove.add(count)
            continue
        seen[id] = count
    return to_remove


def collect(db, expire_time=None, start=0):
    """Finds the objects that should be removed, and removes them from
    the database"""
    to_remove = find_to_remove(db, expire_time, start)
    dest_dir = tempfile.mkdtemp()
    try:
        dest_fn = os.path.join(dest_dir, 'temp.db')
        dest_fn_index = os.path.join(dest_dir, 'temp.db.index')
        db.copy(to_remove, dest_fn, dest_fn_index)
        db.overwrite(dest_fn, dest_fn_index)
    finally:
        shutil.rmtree(dest_dir)
