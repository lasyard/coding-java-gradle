package io.github.lasyard.arrow;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class ArrowCodec {
    private final Schema schema;

    public ArrowCodec(Schema schema) {
        this.schema = schema;
    }

    private static @NonNull RuntimeException unsupportedException(Field filed) {
        return new RuntimeException("Unsupported arrow field type: " + filed);
    }

    private static void initVectorByType(@NonNull FieldVector vector, int rows) {
        switch (vector.getField().getType().getTypeID()) {
            case Int:
                ((IntVector) vector).allocateNew(rows);
                break;
            case Utf8:
                ((VarCharVector) vector).allocateNew(rows);
                break;
            default:
                throw unsupportedException(vector.getField());
        }
    }

    private static void setValueByType(@NonNull ValueVector vector, int row, Object value) {
        switch (vector.getField().getType().getTypeID()) {
            case Int:
                ((IntVector) vector).set(row, (Integer) value);
                break;
            case Utf8:
                ((VarCharVector) vector).set(row, ((String) value).getBytes(StandardCharsets.UTF_8));
                break;
            default:
                throw unsupportedException(vector.getField());
        }
    }

    private static @Nullable Object getValueByType(@NonNull ValueVector vector, int row) {
        Object v = vector.getObject(row);
        switch (vector.getField().getType().getTypeID()) {
            case Int:
                return v;
            case Utf8:
                return new String(((Text) v).getBytes(), StandardCharsets.UTF_8);
            default:
                throw unsupportedException(vector.getField());
        }
    }

    public byte @Nullable [] encode(@NonNull Collection<Object[]> tuples) {
        try (BufferAllocator allocator = new RootAllocator()) {
            try (
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
            ) {
                List<FieldVector> vectorList = vectorSchemaRoot.getFieldVectors();
                int rows = tuples.size();
                for (FieldVector vector : vectorList) {
                    initVectorByType(vector, rows);
                }
                int count = 0;
                for (Object[] tuple : tuples) {
                    for (int i = 0; i < tuple.length; ++i) {
                        ValueVector vector = vectorList.get(i);
                        setValueByType(vector, count, tuple[i]);
                    }
                    ++count;
                }
                vectorSchemaRoot.setRowCount(count);
                try (
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out))
                ) {
                    writer.start();
                    writer.writeBatch();
                    log.info("Number of rows written: " + vectorSchemaRoot.getRowCount());
                    return out.toByteArray();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public List<Object[]> decode(byte[] bytes) {
        try (
            BufferAllocator rootAllocator = new RootAllocator();
            ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), rootAllocator)
        ) {
            List<Object[]> tuples = new LinkedList<>();
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot()) {
                    int rowCount = vectorSchemaRoot.getRowCount();
                    List<FieldVector> vectorList = vectorSchemaRoot.getFieldVectors();
                    for (int count = 0; count < rowCount; ++count) {
                        Object[] tuple = new Object[vectorList.size()];
                        int i = 0;
                        for (FieldVector vector : vectorList) {
                            tuple[i] = getValueByType(vector, count);
                            ++i;
                        }
                        tuples.add(tuple);
                    }
                    log.info("Decoded data:\n{}", vectorSchemaRoot.contentToTSVString());
                }
            }
            return tuples;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
