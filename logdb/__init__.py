import os
from fcntl import lockf as lock_file
from fcntl import LOCK_UN, LOCK_EX
import struct

int_encoding = struct.Struct('<I')
pair_encoding = struct.Struct('<II')


class Database(object):

    INDEX_SIZE = 100

    def __init__(self, filename):
        ## FIXME: a race condition here:
        exists = os.path.exists(filename)
        if not exists:
            self.fp = open(filename, 'w+b')
            self.fp.write(pair_encoding.pack(0, 0))
            self.fp.write('\x00' * (self.INDEX_SIZE * 8))
            # Write a dummy record
            self.fp.write(self._encode_item(0, ''))
        else:
            self.fp = open(filename, 'r+b')

    def _read_start_end(self):
        """Reads the index start/end position from the beginning of
        the file"""
        self.fp.seek(0)
        return pair_encoding.unpack(self.fp.read(8))

    def _write_start_end(self, start, end):
        """Writes the index start/end position to the beginning of the
        file"""
        self.fp.seek(0)
        self.fp.write(pair_encoding.pack(start, end))

    def _increment_start_end(self, start, end):
        """Increments end, and pushes start forward too if necessary"""
        end = (end + 1) % self.INDEX_SIZE
        if end == start:
            start = (start + 1) % self.INDEX_SIZE
        return start, end

    def _index_bytes(self, pos):
        """Given an index position, give the absolute file/byte
        position of that index entry"""
        return pos * 8 + 8

    def _iter_index(self, start, end):
        """Yields (count, bytes) from the index; start and end should be given
        from _read_start_end()"""
        pos = start
        self.fp.seek(self._index_bytes(pos))
        while 1:
            data = self.fp.read(8)
            yield pair_encoding.unpack(data)
            pos += 1
            if pos > self.INDEX_SIZE:
                pos = 0
                self.fp.seek(self._index_bytes(pos))
            if end < start:
                if pos >= end and pos < start:
                    break
            else:
                if pos >= end:
                    break

    def _iter_items(self):
        """Iterates items, from the current file position.  Yields
        (count, data)"""
        while 1:
            data = self.fp.read(4)
            if not data:
                # We're at the end of the file, that's okay
                return
            if len(data) < 4:
                raise Exception('Unexpected end of file when expecting size (%r)' % data)
            size, = int_encoding.unpack(data)
            content = self.fp.read(size - 5)
            data = self.fp.read(4)
            if len(data) < 4:
                raise Exception('Unexpected end of file when expecting count (%r)' % data)
            count, = int_encoding.unpack(data)
            data = self.fp.read(1)
            if data != '\xff':
                raise Exception('Expected \\xff marker (not %r)' % data)
            yield count, content

    def _encode_item(self, count, data):
        return int_encoding.pack(len(data) + 5) + data + int_encoding.pack(count) + '\xff'

    def _read_last_count(self):
        """Read the last count from the file, and position the file at the end"""
        self.fp.seek(-5, os.SEEK_END)
        data = self.fp.read(4)
        self.fp.read(1)
        return int_encoding.unpack(data)[0]

    def _first_bytes(self):
        """Returns (1, byte_pos_of_first_item)"""
        return (1, self.INDEX_SIZE * 8 + 8)

    def extend(self, datas):
        """Appends the data to the database, returning the integer
        counter for the first item in the data
        """
        if not datas:
            raise Exception('You must give some data')
        lock_file(self.fp, LOCK_EX)
        last_count = self._read_last_count()
        self.fp.seek(0, os.SEEK_END)
        first_datas = last_count + 1
        first_datas_bytes = self.fp.tell()
        count = first_datas
        written_length = 0
        for data in datas:
            assert isinstance(data, str)
            enc_data = self._encode_item(count, data)
            written_length += len(enc_data)
            self.fp.write(enc_data)
            count += 1
        start, end = self._read_start_end()
        self.fp.seek(self._index_bytes(end))
        self.fp.write(pair_encoding.pack(first_datas, first_datas_bytes))
        start, end = self._increment_start_end(start, end)
        self._write_start_end(start, end)
        lock_file(self.fp, LOCK_UN, 0, 4)
        return first_datas

    def read(self, seek, length=-1):
        """Read items from the database, starting from count, max
        length (or -1 means to the end)

        yields (count, data)
        """
        assert isinstance(seek, int)
        assert seek > 0
        if length == 0:
            return
        start, end = self._read_start_end()
        best = self._first_bytes()
        for count, bytes in self._iter_index(start, end):
            if count > seek:
                break
            best = count, bytes
        self.fp.seek(best[1])
        for count, data in self._iter_items():
            if count == seek:
                yield count, data
                length -= 1
                break
        if not length:
            return
        for count, data in self._iter_items():
            yield count, data
            length -= 1
            if not length:
                return

    def length(self):
        return self._read_last_count()
