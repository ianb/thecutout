import os
import shutil
from fcntl import lockf as lock_file
from fcntl import LOCK_UN, LOCK_EX
import struct
from contextlib import contextmanager

int_encoding = struct.Struct('<I')
triple_encoding = struct.Struct('<III')


class ExpectationFailed(Exception):
    pass


class TruncatedFile(Exception):
    pass


class Database(object):

    def __init__(self, data_filename, index_filename=None):
        if index_filename is None:
            index_filename = data_filename + '.index'
        self.index_filename = index_filename
        self.data_filename = data_filename
        try:
            self.index_fp = open(index_filename, 'r+b')
        except IOError, e:
            if e.errno != 2:
                raise
            ## File does not exist
            try:
                fd = os.open(index_filename, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_EXLOCK)
            except IOError, e:
                if e.errno != 17:
                    raise
                ## File was created while we were trying to create it, which is fine
                self.index_fp = open(index_filename, 'r+b')
            else:
                self.index_fp = os.fdopen(fd, 'r+b')
                self.index_fp.write(triple_encoding.pack(0, 0, 0))
                lock_file(self.index_fp, LOCK_UN, 0, 0, os.SEEK_SET)
        fd = os.open(data_filename, os.O_RDWR | os.O_CREAT)
        self.data_fp = os.fdopen(fd, 'r+b')

    def _read_last_count(self):
        """Reads the counter of the last item appended"""
        self.index_fp.seek(-4, os.SEEK_END)
        if self.index_fp.tell() % 4:
            raise Exception("Misaligned length of index file %s" % self.index_filename)
        chunk = self.index_fp.read(4)
        if not chunk:
            # The file has been truncated, there's not even the 0/0/0 record
            raise TruncatedFile()
        return int_encoding.unpack(chunk)[0]

    def _seek_index(self, seek_count):
        """Seeks the index file to immediately *after* seek_count.
        May be the end of the file."""
        last = self._read_last_count()
        if last <= seek_count:
            return
        last_pos = self.index_fp.tell() / 12
        # Let's guess where we should go...
        ## FIXME: might be better to count back absolutely, instead of
        ## using the range
        guess = last_pos * seek_count / last
        self.index_fp.seek(guess * 12 + 8)
        least = 0
        greatest = last_pos
        while 1:
            chunk = self.index_fp.read(4)
            if len(chunk) < 4:
                return
            count, = int_encoding.unpack(chunk)
            if count == seek_count:
                return
            if seek_count > count:
                least = guess
            else:
                greatest = guess
            if greatest - least < 1:
                # either we've bounded the target, or we've bounded an
                # empty spot (meaning the target doesn't exist, but
                # we're okay)
                if greatest != guess:
                    # We're not at the right place yet
                    self.index_fp.seek(greatest * 12)
                return
            diff = seek_count - count
            self.index_fp.seek(diff * 12 - 4, os.SEEK_CUR)

    def extend(self, datas, expect_latest=None, expect_last_counter=None,
               with_counters=False):
        """Appends the data to the database, returning the integer
        counter for the first item in the data
        """
        with lock_append(self.index_fp):
            count = self._read_last_count()
            if expect_latest is not None and count > expect_latest:
                raise ExpectationFailed
            if expect_last_counter is not None and count != expect_last_counter:
                raise ExpectationFailed
            first_datas = None
            self.index_fp.seek(0, os.SEEK_END)
            self.data_fp.seek(0, os.SEEK_END)
            pos = self.data_fp.tell()
            for data in datas:
                if with_counters:
                    next_count, data = data
                    assert next_count > count, "Bad next count: %r (should be greater than %r)" % (next_count, count)
                    count = next_count
                else:
                    count += 1
                if first_datas is None:
                    first_datas = count
                assert isinstance(data, str)
                length = len(data)
                self.data_fp.write(data)
                self.index_fp.write(triple_encoding.pack(length, pos, count))
                pos += length
            return first_datas

    def read(self, above, last=-1):
        """Yields items starting at `above` and until (and including)
        `last` if it is given"""
        assert isinstance(above, int)
        assert above >= 0
        self._seek_index(above)
        last_pos = None
        while 1:
            chunk = self.index_fp.read(12)
            if not chunk or len(chunk) < 12:
                break
            length, pos, count = triple_encoding.unpack(chunk)
            assert count > above, "failed: count=%r > above=%r; chunk=%r; tell=%r; trip=%r" % (count, above, chunk, self.index_fp.tell(), [length, pos, count, self.index_filename, self.index_fp.seek(0) or self.index_fp.read(), self.data_fp.seek(0) or self.data_fp.read()])
            if last_pos is None:
                self.data_fp.seek(pos)
                last_pos = pos
            else:
                # We should be reading forward, so this should be correct
                assert last_pos == pos
            data = self.data_fp.read(length)
            if len(data) < length:
                # Truncated record, we caught someone in the process of reading
                # But this must be the last complete record
                break
            last_pos += length
            yield count, data
            if last > 0 and last <= count:
                break

    def get_file_positions(self, until):
        """Return (index_position, database_position) where the
        position is the start of the record `until`, or whatever
        record is next (if until is missing).

        This can be used to establish a chunk of the database that
        represents a range."""
        if until is None:
            ## FIXME: Or should I seek and tell?  Could they be different?
            return (os.path.getsize(self.index_filename),
                    os.path.getsize(self.data_filename))
        self._seek_index(until)
        index_pos = self.index_fp.tell()
        chunk = self.index_fp.read(12)
        if not chunk:
            # until doesn't exist
            return (index_pos, os.path.getsize(self.data_filename))
        length, pos, count = triple_encoding.unpack(chunk)
        return (index_pos, pos)

    def clear(self):
        ## FIXME: not sure the concurrency effect is here.  It's not
        ## intended to be concurrent really.  Could mostly do weird things
        ## to readers.
        with lock_complete(self.index_fp):
            self.index_fp.seek(12, os.SEEK_SET)
            self.index_fp.truncate()
            self.data_fp.seek(0, os.SEEK_SET)
            self.data_fp.truncate()

    def length(self):
        return self._read_last_count()

    def copy(self, exclude_counts, dest_filename, dest_index_filename=None):
        """Copies this database to a new database, but excluding the
        excluded counts.

        exclude_counts should be a set-like object (which can include a list
        or dictionary, but a set is best)."""
        ## We use an exclude list, because if you don't know about an item then
        ## we should copy it over.
        ## FIXME: we might read someone else's partial-write.  But if we lock, get the
        ## self.index_fp.tell(), and don't read past that, we should be okay.
        if dest_index_filename is None:
            dest_index_filename = dest_filename + '.index'
        data_fp = open(dest_filename, 'wb')
        index_fp = open(dest_index_filename, 'wb')
        self.index_fp.seek(0)
        self.data_fp.seek(0)
        data_fp_pos = 0
        while 1:
            chunk = self.index_fp.read(12)
            if not chunk:
                break
            length, pos, count = triple_encoding.unpack(chunk)
            if count in exclude_counts:
                assert count != 0
                self.data_fp.seek(length, os.SEEK_CUR)
                continue
            assert self.data_fp.tell() == pos
            index_fp.write(triple_encoding.pack(length, data_fp_pos, count))
            data = self.data_fp.read(length)
            assert len(data) == length
            data_fp.write(data)
            data_fp_pos += length
        data_fp.close()
        index_fp.close()

    def overwrite(self, data_filename, index_filename):
        """Overwrites this database with the given files"""
        with lock_complete(self.index_fp):
            self.index_fp.seek(0)
            # First make sure no one can do anything:
            self.index_fp.truncate()
            self.data_fp.seek(0)
            self.data_fp.truncate()
            ## FIXME: should I truncate (or re-truncate) them large,
            ## to pre-allocate space?  Readers would currently get
            ## confused.
            with open(index_filename, 'rb') as fp:
                shutil.copyfile(fp, self.index_fp)
            with open(data_filename, 'rb') as fp:
                shutil.copyfile(fp, self.data_fp)
            ## FIXME: should I use any renames?
            ## I could truncate the old files to invalidate them, then
            ## rename both?

    def delete(self):
        self.close()
        os.unlink(self.index_filename)
        os.unlink(self.data_filename)

    def close(self):
        self.index_fp.close()
        self.data_fp.close()


@contextmanager
def lock_append(fp):
    lock_file(fp, LOCK_EX, 0, 0, os.SEEK_END)
    yield
    lock_file(fp, LOCK_UN, 0, 0, os.SEEK_END)


@contextmanager
def lock_complete(fp):
    lock_file(fp, LOCK_EX, 0, 0, os.SEEK_SET)
    yield
    lock_file(fp, LOCK_UN, 0, 0, os.SEEK_SET)
