import os
from fcntl import lockf as lock_file
from fcntl import LOCK_UN, LOCK_EX
import struct

int_encoding = struct.Struct('<I')
pair_encoding = struct.Struct('<II')


class Database(object):

    def __init__(self, filename):
        ## FIXME: a race condition here:
        self.filename = filename
        exists = os.path.exists(filename)
        if not exists:
            self.fp = open(filename, 'w+b')
            # Write a dummy record
            self.fp.write(self._encode_item(0, ''))
        else:
            self.fp = open(filename, 'r+b')

    def _encode_item(self, count, data):
        length = len(data)
        return int_encoding.pack(length) + data + pair_encoding.pack(length, count)

    def _fetch_item(self, prev_start_pos=None):
        """Iterates items, in reverse, from the current file position.
        It can start from an explicit position, or the current
        position (default=None).  A position of -1 means the end of
        the file.  The position should point to the entry immediately
        after the one you want to fetch.

        Returns (count, data, start_pos)

        Leaves the file position in an unuseful position.
        """
        if prev_start_pos == -1:
            self.fp.seek(-8, os.SEEK_END)
        elif prev_start_pos is None:
            self.fp.seek(-8, os.SEEK_CUR)
        else:
            self.fp.seek(prev_start_pos - 8)
        data = self.fp.read(8)
        size, count = pair_encoding.unpack(data)
        self.fp.seek(-size - 12, os.SEEK_CUR)
        start_pos = self.fp.tell()
        content = self.fp.read(size + 4)
        size_check, = int_encoding.unpack(content[:4])
        if size_check != size:
            raise Exception('Size at start and end do not match (%r!=%r, pos=%r)'
                            % (size_check, size, self.fp.tell() - size - 4))
        return count, content[4:], start_pos

    def extend(self, datas):
        """Appends the data to the database, returning the integer
        counter for the first item in the data
        """
        if not datas:
            raise Exception('You must give some data')
        lock_file(self.fp, LOCK_EX)
        self.fp.seek(-4, os.SEEK_END)
        last_count, = int_encoding.unpack(self.fp.read(4))
        first_datas = last_count + 1
        count = first_datas
        for data in datas:
            assert isinstance(data, str)
            enc_data = self._encode_item(count, data)
            self.fp.write(enc_data)
            count += 1
        lock_file(self.fp, LOCK_UN)
        return first_datas

    def read(self, least):
        """Yields (count, data), in reverse, up to but not including least
        """
        assert isinstance(least, int)
        assert least >= 0
        pos = -1
        while 1:
            count, data, pos = self._fetch_item(pos)
            if count <= least:
                return
            yield count, data

    def clear(self):
        self.fp.close()
        self.fp = open(self.filename, 'w+b')
        # Write a dummy record
        self.fp.write(self._encode_item(0, ''))

    def length(self):
        self.fp.seek(-4, os.SEEK_END)
        return int_encoding.unpack(self.fp.read(4))[0]
