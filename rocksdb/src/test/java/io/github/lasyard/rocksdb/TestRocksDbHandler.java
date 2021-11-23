package io.github.lasyard.rocksdb;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRocksDbHandler {
    private static final String DB = "rocksdb/";

    @AfterAll
    public static void cleanUpAll() throws IOException {
        FileUtils.deleteDirectory(new File(DB));
    }

    @Test
    public void testOpenClose() {
        try (RocksDbHandler handler = RocksDbHandler.open("rocksdb/")) {
            assertThat(handler).isNotNull();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteRead() {
        try (RocksDbHandler handler = RocksDbHandler.open("rocksdb/")) {
            handler.put("test".getBytes(), "Peso".getBytes());
            String v = new String(handler.get("test".getBytes()), StandardCharsets.UTF_8);
            assertThat(v).isEqualTo("Peso");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testEnumerate() {
        try (RocksDbHandler handler = RocksDbHandler.open("rocksdb/")) {
            handler.put("test".getBytes(), "Peso".getBytes());
            handler.put("test1".getBytes(), "Dashi".getBytes());
            for (Map.Entry<byte[], byte[]> entry : handler) {
                System.out.println(new String(entry.getKey()));
                System.out.println(new String(entry.getValue()));
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
