package io.github.lasyard.avatica.local;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.github.lasyard.avatica.schema.MockSchema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

public class LocalMeta extends MetaImpl {
    public static final String TEST_STRING = "Hello, dude.";

    // Why meta is bound to connection?
    public LocalMeta(LocalConnection connection) {
        super(connection);
    }

    private static RelDataType makeStruct(RelDataTypeFactory typeFactory, @Nonnull RelDataType type) {
        if (type.isStruct()) {
            return type;
        }
        return typeFactory.builder().add("$0", type).build();
    }

    @Nonnull
    private static List<ColumnMetaData> getColumnMetaDataList(
        JavaTypeFactory typeFactory,
        @Nonnull RelDataType jdbcType,
        List<? extends @Nullable List<String>> originList
    ) {
        List<RelDataTypeField> fieldList = jdbcType.getFieldList();
        final List<ColumnMetaData> columns = new ArrayList<>(fieldList.size());
        for (int i = 0; i < fieldList.size(); ++i) {
            RelDataTypeField field = fieldList.get(i);
            columns.add(metaData(
                typeFactory,
                columns.size(),
                field.getName(),
                field.getType(),
                originList.get(i)
            ));
        }
        return columns;
    }

    @Nonnull
    private static ColumnMetaData metaData(
        @Nonnull JavaTypeFactory typeFactory,
        int ordinal,
        String fieldName,
        @Nonnull RelDataType type,
        @Nullable List<String> origins
    ) {
        final ColumnMetaData.AvaticaType aType = ColumnMetaData.scalar(
            type.getSqlTypeName().getJdbcOrdinal(),
            fieldName,
            ColumnMetaData.Rep.of(typeFactory.getJavaClass(type))
        );
        return new ColumnMetaData(
            ordinal,
            false,
            true,
            false,
            false,
            type.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
            true,
            type.getPrecision(),
            fieldName,
            origin(origins, 0),
            origin(origins, 2),
            type.getPrecision(),
            type.getScale(),
            origin(origins, 1),
            null,
            aType,
            true,
            false,
            false,
            aType.columnClassName());
    }

    private static @Nullable String origin(@Nullable List<String> origins, int offsetFromEnd) {
        return origins == null || offsetFromEnd >= origins.size()
            ? null : origins.get(origins.size() - 1 - offsetFromEnd);
    }

    private static int getScale(@Nonnull RelDataType type) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
            ? 0
            : type.getScale();
    }

    private static int getPrecision(@Nonnull RelDataType type) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
            ? 0
            : type.getPrecision();
    }

    @Override
    public StatementHandle prepare(
        @Nonnull ConnectionHandle ch,
        String sql,
        long maxRowCount
    ) {
        try {
            final Signature signature = parseQuery(sql);
            StatementHandle h = createStatement(ch);
            h.signature = signature;
            return h;
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private Signature parseQuery(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, SqlParser.config());
        SqlNode sqlNode = parser.parseQuery(sql);
        if (sqlNode.getKind() != SqlKind.SELECT) {
            throw new RuntimeException("Only select of constant values is supported.");
        }
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(
            true,
            true,
            "xxx",
            new MockSchema()
        );
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList("xxx"),
            typeFactory,
            CalciteConnectionConfig.DEFAULT
        );
        SqlValidator sqlValidator = SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT
        );
        sqlNode = sqlValidator.validate(sqlNode);
        Meta.StatementType statementType = Meta.StatementType.SELECT;
        RelDataType type = sqlValidator.getValidatedNodeType(sqlNode);
        RelDataType jdbcType = makeStruct(typeFactory, type);
        List<List<String>> originList = sqlValidator.getFieldOrigins(sqlNode);
        final List<ColumnMetaData> columns = getColumnMetaDataList(typeFactory, jdbcType, originList);
        RelDataType parameterRowType = sqlValidator.getParameterRowType(sqlNode);
        final List<AvaticaParameter> parameters = new ArrayList<>();
        for (RelDataTypeField field : parameterRowType.getFieldList()) {
            RelDataType fieldType = field.getType();
            SqlTypeName sqlTypeName = type.getSqlTypeName();
            parameters.add(
                new AvaticaParameter(
                    false,
                    getPrecision(fieldType),
                    getScale(fieldType),
                    sqlTypeName.getJdbcOrdinal(),
                    sqlTypeName.getName(),
                    Object.class.getName(),
                    field.getName()));
        }
        final Meta.CursorFactory cursorFactory = Meta.CursorFactory.ARRAY;
        return new Signature(
            columns,
            sql,
            parameters,
            null,
            cursorFactory,
            statementType
        );
    }

    @Deprecated
    @Override
    public ExecuteResult prepareAndExecute(
        StatementHandle sh,
        String sql,
        long maxRowCount,
        PrepareCallback callback
    ) {
        throw new RuntimeException("This method is deprecated.");
    }

    @Override
    public ExecuteResult prepareAndExecute(
        @Nonnull StatementHandle sh,
        String sql,
        long maxRowCount,
        int maxRowsInFirstFrame,
        @Nonnull PrepareCallback callback
    ) {
        try {
            final Signature signature = parseQuery(sql);
            sh.signature = signature;
            int updateCount = -1; // SELECT and DML produces result set
            synchronized (callback.getMonitor()) {
                callback.clear();
                callback.assign(signature, null, updateCount);
            }
            callback.execute();
            final MetaResultSet metaResultSet = MetaResultSet.create(
                sh.connectionId,
                sh.id,
                false,
                signature,
                null,
                updateCount
            );
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (SQLException | SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(
        StatementHandle sh,
        List<String> sqlCommands
    ) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult executeBatch(
        StatementHandle sh,
        List<List<TypedValue>> parameterValues
    ) throws NoSuchStatementException {
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Frame fetch(
        @Nonnull StatementHandle sh,
        long offset,
        int fetchMaxRowCount
    ) {
        final CursorFactory cursorFactory = sh.signature.cursorFactory;
        final Iterator iterator = Iterators.singletonIterator(new Object[]{TEST_STRING});
        final List rows = MetaImpl.collect(cursorFactory, iterator, new ArrayList<>());
        boolean done = fetchMaxRowCount == 0 || rows.size() < fetchMaxRowCount;
        return new Frame(offset, done, rows);
    }

    @Deprecated
    @Override
    public ExecuteResult execute(
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        long maxRowCount
    ) throws NoSuchStatementException {
        throw new AssertionError("This method is deprecated.");
    }

    @Override
    public ExecuteResult execute(
        @Nonnull StatementHandle sh,
        List<TypedValue> parameterValues,
        int maxRowsInFirstFrame
    ) throws NoSuchStatementException {
        int updateCount = -1; // SELECT and DML produces result set
        final MetaResultSet metaResultSet = MetaResultSet.create(
            sh.connectionId,
            sh.id,
            false,
            sh.signature,
            null,
            updateCount
        );
        return new ExecuteResult(ImmutableList.of(metaResultSet));
    }

    @Override
    public void closeStatement(StatementHandle sh) {
    }

    @Override
    public boolean syncResults(
        StatementHandle sh,
        QueryState state,
        long offset
    ) throws NoSuchStatementException {
        return false;
    }

    @Override
    public void commit(ConnectionHandle ch) {
    }

    @Override
    public void rollback(ConnectionHandle ch) {
    }
}
