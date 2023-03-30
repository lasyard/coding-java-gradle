package io.github.lasyard.arrow;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestArrowCodec {
    private static final List<Object[]> originalTuples = Arrays.asList(
        new Object[]{"Alice", 15},
        new Object[]{"Betty", 18},
        new Object[]{"Cindy", 20},
        new Object[]{"Doris", 22},
        new Object[]{"Emily", 24}
    );

    @Test
    public void testEncode() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        ArrowCodec codec = new ArrowCodec(new Schema(Arrays.asList(name, age)));
        byte[] out = codec.encode(originalTuples);
        log.info("Length of encoded byte array: {}", out.length);
        List<Object[]> decodedTuples = codec.decode(out);
        assertThat(decodedTuples).hasSameElementsAs(originalTuples);
    }

    @Test
    public void testEncodedLength() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        ArrowCodec codec = new ArrowCodec(new Schema(Arrays.asList(name, age)));
        int[] len = new int[6];
        List<Object[]> encodingTuples = new LinkedList<>();
        for (int i = 0; i < len.length; ++i) {
            len[i] = Objects.requireNonNull(codec.encode(encodingTuples)).length;
            if (i < originalTuples.size()) {
                encodingTuples.add(originalTuples.get(i));
            } else {
                break;
            }
        }
        log.info("Lengths of encoded byte array: {}", len);
    }
}
