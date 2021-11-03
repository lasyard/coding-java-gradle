package io.github.lasyard.calcite;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

@Slf4j
public class MockSchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        log.info("schemaPlus = {}, name = {}, map = {}", schemaPlus, name, operand);
        TableFlavor flavor = TableFlavor.of((String) operand.get("flavor"));
        return new MockSchema(flavor);
    }
}
