package io.github.lasyard.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;

public class RocksDbHandler implements AutoCloseable, Iterable<Map.Entry<byte[], byte[]>> {
    static {
        RocksDB.loadLibrary();
    }

    private final Options options;
    private final RocksDB db;

    public RocksDbHandler(String path) throws RocksDBException {
        options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, path);
    }

    @Nonnull
    public static RocksDbHandler open(String path) throws RocksDBException {
        return new RocksDbHandler(path);
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    @Override
    public void close() {
        options.close();
        db.close();
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        // TODO: where to call `iterator.close()`?
        final RocksIterator iterator = db.newIterator();
        // This is crucial
        iterator.seekToFirst();
        return new Iterator<Map.Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                final byte[] key = iterator.key();
                final byte[] value = iterator.value();
                iterator.next();
                return new Map.Entry<byte[], byte[]>() {
                    @Override
                    public byte[] getKey() {
                        return key;
                    }

                    @Override
                    public byte[] getValue() {
                        return value;
                    }

                    @Override
                    public byte[] setValue(byte[] value) {
                        return null;
                    }

                    @Override
                    public boolean equals(Object obj) {
                        return false;
                    }

                    @Override
                    public int hashCode() {
                        return 0;
                    }
                };
            }
        };
    }
}
