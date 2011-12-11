import os
from fcntl import lockf as lock_file
from fcntl import LOCK_UN, LOCK_EX
import struct

int_encoding = struct.Struct('<I')
triple_encoding = struct.Struct('<III')


class Database(object):

    def __init__(self, data_filename, index_filename=None):
        if index_filename is None:
            index_filename = data_filename + '.index'
        self.index_filename = index_filename
        self.data_filename = data_filename
        if not os.path.exists(data_filename):
            self.data_fp = open(data_filename, 'w+b')
            self.index_fp = open(index_filename, 'w+b')
            # Write a dummy record
            self.index_fp.write(triple_encoding.pack(0, 0, 0))
        else:
            self.data_fp = open(data_filename, 'r+b')
            self.index_fp = open(index_filename, 'r+b')

    def _read_last_count(self):
        """Reads the counter of the last item appended"""
        self.index_fp.seek(-4, os.SEEK_END)
        return int_encoding.unpack(self.index_fp.read(4))[0]

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
            count, = int_encoding.unpack(self.index_fp.read(4))
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
                    os.seek(greatest * 12)
                return
            diff = seek_count - count
            self.index_fp.seek(diff * 12 - 4, os.SEEK_CUR)

    def extend(self, datas):
        """Appends the data to the database, returning the integer
        counter for the first item in the data
        """
        lock_file(self.index_fp, LOCK_EX)
        count = self._read_last_count()
        count += 1
        first_datas = count
        self.index_fp.seek(0, os.SEEK_END)
        self.data_fp.seek(0, os.SEEK_END)
        pos = self.data_fp.tell()
        for data in datas:
            assert isinstance(data, str)
            length = len(data)
            self.data_fp.write(data)
            self.index_fp.write(triple_encoding.pack(length, pos, count))
            count += 1
            pos += length
        lock_file(self.index_fp, LOCK_UN)
        return first_datas

    def read(self, above, last=-1):
        assert isinstance(above, int)
        assert above >= 0
        self._seek_index(above)
        last_pos = None
        while 1:
            chunk = self.index_fp.read(12)
            if not chunk:
                break
            length, pos, count = triple_encoding.unpack(chunk)
            assert count > above
            if last_pos is None:
                self.data_fp.seek(pos)
                last_pos = pos
            else:
                # We should be reading forward, so this should be correct
                assert last_pos == pos
            data = self.data_fp.read(length)
            assert len(data) == length
            last_pos += length
            yield count, data
            if last > 0 and last <= count:
                break

    def clear(self):
        self.data_fp.close()
        self.index_fp.close()
        self.data_fp = open(self.data_filename, 'w+b')
        self.index_fp = open(self.index_filename, 'w+b')
        self.index_fp.write(triple_encoding.pack(0, 0, 0))

    def length(self):
        return self._read_last_count()
