/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package com.micromux.cassandra.jdbc;

import com.datastax.driver.core.*;
import com.micromux.cassandra.jdbc.meta.CassandraColumn;
import com.micromux.cassandra.jdbc.meta.CassandraResultSetMetaData;
import com.micromux.cassandra.jdbc.meta.CassandraRow;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class MetadataResultSets extends AbstractResultSet implements ResultSet {

    static final String TABLE_CONSTANT = "TABLE";
    static final String VIEW_CONSTANT = "VIEW";

    // array backed results - navigate using rowId
    private final List<CassandraRow> rows = new ArrayList<CassandraRow>();

    // current row id
    private int rowId = -1;

    // was the last value null?
    private boolean wasNull = false;

    // Private Constructor
    private MetadataResultSets() {
    }

    /**
     * Add a new row to the resultset.
     *
     * @param row Row to add to the resultset.
     */
    void addRow(CassandraRow row) {
        this.rows.add(row);
    }

    /**
     * Add a new row to the resultset as one of the first records. This is used
     * for primary key columns.
     *
     * @param row Row to add first to the resultset.
     */
    void addFirst(CassandraRow row) {
        this.rows.add(0, row);
    }

    /**
     * The table types available in this database.
     *
     * @param statement Statement that needs meta-data.
     * @return The resultset containing the available table types.
     * @throws SQLException Fatal database error.
     */
    public static ResultSet makeTableTypes(CassandraStatement statement) throws SQLException {

        final MetadataResultSets metaResults = new MetadataResultSets();

        CassandraColumn<String> tableTypeCol = new CassandraColumn<>("TABLE_TYPE", TABLE_CONSTANT);
        metaResults.addRow(new CassandraRow(tableTypeCol));

        CassandraColumn<String> viewTypeCol = new CassandraColumn<>("VIEW_TYPE", VIEW_CONSTANT);
        metaResults.addRow(new CassandraRow(viewTypeCol));

        return metaResults;

    }

    public static ResultSet makeCatalogs(CassandraStatement statement) throws SQLException {

        CassandraColumn<String> tableCat = new CassandraColumn<String>("TABLE_CAT", statement.connection.getCatalog());
        CassandraRow row = new CassandraRow(tableCat);

        MetadataResultSets metaResults = new MetadataResultSets();
        metaResults.addRow(row);

        return metaResults;

    }

    /**
     * Retrieves the schema names available in this database.  The results
     * are ordered by <code>TABLE_CATALOG</code> and
     * <code>TABLE_SCHEM</code>.
     * <p/>
     * <P>The schema columns are:
     * <OL>
     * <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
     * <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be <code>null</code>)
     * </OL>
     *
     * @param statement     Statement to base schema query
     * @param schemaPattern Pattern to apply which may include SQL wildcarding; use {@code null} for open match.
     * @return a <code>ResultSet</code> object in which each row is a
     * schema description
     * @throws SQLException if a database access error occurs
     */
    public static ResultSet makeSchemas(final CassandraStatement statement,
                                        final String schemaPattern) throws SQLException {

        final Cluster cluster = statement.connection.getCluster();
        final MetadataResultSets metaResult = new MetadataResultSets();

        // TABLE_CATALOG String => catalog name (may be null)
        // TABLE_SCHEM String => schema name
        for (KeyspaceMetadata keyspace : cluster.getMetadata().getKeyspaces()) {

            if (hasPatternMatch(keyspace.getName(), schemaPattern)) {

                CassandraColumn<String> entryCatalog = new CassandraColumn<>("TABLE_CATALOG", cluster.getClusterName());
                CassandraColumn<String> entrySchema = new CassandraColumn<>("TABLE_SCHEM", keyspace.getName());

                CassandraRow row = new CassandraRow(entrySchema, entryCatalog);
                metaResult.addRow(row);

            }

        }

        return metaResult;

    }

    /**
     * Query the list of available tables in the schema. This translates to column groups in Cassandra.
     * This must match for the format expected by JDBC. Note the pattern values may specify SQL wildcard
     * patterns such as "%".
     *
     * @param statement        Statement to use for query. This identifies the session to be used to execute CQL.
     * @param schemaPattern    Pattern to match on; this may be {@code null}.
     * @param tableNamePattern Pattern to match on; this may be {@code null}.
     * @param typeSet          Set of types to query (TABLE | VIEW)
     * @return Navigable resultset that can be used to review the results of the table query.
     * @throws SQLException Fatal database error.
     */
    public static ResultSet makeTables(final CassandraStatement statement,
                                       final String schemaPattern,
                                       final String tableNamePattern,
                                       final Set<String> typeSet) throws SQLException {

        //   1.   TABLE_CAT String => table catalog (may be null)
        //   2.   TABLE_SCHEM String => table schema (may be null)
        //   3.   TABLE_NAME String => table name
        //   4.   TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
        //   5.   REMARKS String => explanatory comment on the table
        //   6.   TYPE_CAT String => the types catalog (may be null)
        //   7.   TYPE_SCHEM String => the types schema (may be null)
        //   8.   TYPE_NAME String => type name (may be null)
        //   9.   SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
        //   10.  REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)        

        // create returned resultset
        final MetadataResultSets metaResults = new MetadataResultSets();

        final Cluster cluster = statement.connection.getCluster();
        Metadata metadata = cluster.getMetadata();

        // enumerate all keyspaces
        for (KeyspaceMetadata keyspace : metadata.getKeyspaces()) {

            // accept this schema (keyspace) if it matches the pattern
            if (hasPatternMatch(keyspace.getName(), schemaPattern)) {

                // now look for tables and/or views to accept
                if (null == typeSet || typeSet.isEmpty() || typeSet.contains(TABLE_CONSTANT)) {
                    for (TableMetadata table : keyspace.getTables()) {

                        if (hasPatternMatch(table.getName(), tableNamePattern)) {

                            CassandraRow row = createCassandraRow(keyspace,
                                    TABLE_CONSTANT,
                                    table.getName(),
                                    (null == table.getOptions()) ? "" : table.getOptions().getComment());

                            metaResults.addRow(row);

                        }
                    }
                }

                if (null == typeSet || typeSet.isEmpty() || typeSet.contains(VIEW_CONSTANT)) {
                    for (MaterializedViewMetadata view : keyspace.getMaterializedViews()) {

                        if (hasPatternMatch(view.getName(), tableNamePattern)) {

                            CassandraRow row = createCassandraRow(keyspace,
                                    VIEW_CONSTANT,
                                    view.getName(),
                                    (null == view.getOptions()) ? "" : view.getOptions().getComment());

                            metaResults.addRow(row);
                        }
                    }
                }

            }
        }

        return metaResults;

    }

    private static CassandraRow createCassandraRow(final KeyspaceMetadata keyspace,
                                                   final String tableType,
                                                   final String tableName,
                                                   final String tableComments) {

        CassandraColumn<String> entryCatalog = new CassandraColumn<>("TABLE_CAT", keyspace.getName());
        CassandraColumn<String> entryTypeCatalog = new CassandraColumn<>("TYPE_CAT", "");
        CassandraColumn<String> entryTypeSchema = new CassandraColumn<>("TYPE_SCHEM", "");
        CassandraColumn<String> entryTypeName = new CassandraColumn<>("TYPE_NAME", "");
        CassandraColumn<String> entrySRCN = new CassandraColumn<>("SELF_REFERENCING_COL_NAME", "");
        CassandraColumn<String> entryRefGeneration = new CassandraColumn<>("REF_GENERATION", "");
        CassandraColumn<String> entryTableType = new CassandraColumn<>("TABLE_TYPE", tableType);
        CassandraColumn<String> entrySchema = new CassandraColumn<>("TABLE_SCHEM", keyspace.getName());
        CassandraColumn<String> entryTableName = new CassandraColumn<>("TABLE_NAME", tableName);
        CassandraColumn<String> entryRemarks = new CassandraColumn<>("REMARKS", tableComments);

        return new CassandraRow(entryCatalog,
                entrySchema,
                entryTableName,
                entryTableType,
                entryRemarks,
                entryTypeCatalog,
                entryTypeSchema,
                entryTypeName,
                entrySRCN,
                entryRefGeneration);

    }

    /**
     * Query the column schema for the specified table name pattern. Note that values specified on the pattern
     * match may include SQL wildcard symbols such as "%" which need to be interpreted.
     *
     * @param statement             Valid statement with Connection.
     * @param keyspaceNamePattern   Optional schema pattern or {@code null}
     * @param tableNamePattern      Optional table name pattern or {@code null} for all.
     * @param columnNamePattern     Optional column name pattern or {@code null} for all.
     * @return Results with column information.
     * @throws SQLException
     */
    public static ResultSet makeColumns(final CassandraStatement statement,
                                        final String keyspaceNamePattern,
                                        final String tableNamePattern,
                                        final String columnNamePattern) throws SQLException, CharacterCodingException {

        // create the resultsets that will return...
        final MetadataResultSets metaResults = new MetadataResultSets();

        // 1.TABLE_CAT String => table catalog (may be null)
        // 2.TABLE_SCHEM String => table schema (may be null)
        // 3.TABLE_NAME String => table name
        // 4.COLUMN_NAME String => column name
        // 5.DATA_TYPE int => SQL type from java.sql.Types
        // 6.TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
        // 7.COLUMN_SIZE int => column size.
        // 8.BUFFER_LENGTH is not used.
        // 9.DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
        // 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
        // 11.NULLABLE int => is NULL allowed. - columnNoNulls - might not allow NULL values
        // - columnNullable - definitely allows NULL values
        // - columnNullableUnknown - nullability unknown
        //
        // 12.REMARKS String => comment describing column (may be null)
        // 13.COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in
        // single quotes (may be null)
        // 14.SQL_DATA_TYPE int => unused
        // 15.SQL_DATETIME_SUB int => unused
        // 16.CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
        // 17.ORDINAL_POSITION int => index of column in table (starting at 1)
        // 18.IS_NULLABLE String => ISO rules are used to determine the nullability for a column. - YES --- if the parameter can include
        // NULLs
        // - NO --- if the parameter cannot include NULLs
        // - empty string --- if the nullability for the parameter is unknown
        //
        // 19.SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
        // 20.SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
        // 21.SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)
        // 22.SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if
        // DATA_TYPE isn't DISTINCT or user-generated REF)
        // 23.IS_AUTOINCREMENT String => Indicates whether this column is auto incremented - YES --- if the column is auto incremented
        // - NO --- if the column is not auto incremented
        // - empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown
        // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column ï YES --- if this a generated column
        // - NO --- if this not a generated column
        // - empty string --- if it cannot be determined whether this is a generated column

        final Cluster cluster = statement.connection.getCluster();

        for (KeyspaceMetadata keyspace : cluster.getMetadata().getKeyspaces()) {

            // find matching table structures
            for (TableMetadata table : findTableMetaData(keyspace, keyspaceNamePattern, tableNamePattern)) {
                appendCassandraRow(metaResults, cluster, table, columnNamePattern);
            }

            // find matching view structures
            for (MaterializedViewMetadata view : findViewMetaData(keyspace, keyspaceNamePattern, tableNamePattern)) {
                appendCassandraRow(metaResults, cluster, view, columnNamePattern);
            }

        }

        return metaResults;

    }

    private static void appendCassandraRow(final MetadataResultSets metaResults,
                                           final Cluster cluster,
                                           final AbstractTableMetadata table,
                                           final String columnNamePattern) {

        // identify primary key columns
        final List<String> pks = new LinkedList<>();

        for (ColumnMetadata pk : table.getPrimaryKey()) {
            pks.add(pk.getName());
        }

        // construct non-varient meta-data results
        CassandraColumn<String> entryCatalog = new CassandraColumn<>("TABLE_CAT", cluster.getClusterName());

        CassandraColumn<Integer> entryBufferLength = new CassandraColumn<>("BUFFER_LENGTH", 0);
        CassandraColumn<String> entryRemarks = new CassandraColumn<>("REMARKS", "");
        CassandraColumn<String> entryColumnDef = new CassandraColumn<>("COLUMN_DEF", "");
        CassandraColumn<String> entrySQLDataType = new CassandraColumn<>("SQL_DATA_TYPE", "");
        CassandraColumn<Integer> entrySQLDateTimeSub = new CassandraColumn<>("SQL_DATETIME_SUB", 0);
        CassandraColumn<String> entryScopeCatalog = new CassandraColumn<>("SCOPE_CATLOG", "");
        CassandraColumn<String> entryScopeSchema = new CassandraColumn<>("SCOPE_SCHEMA", "");
        CassandraColumn<String> entryScopeTable = new CassandraColumn<>("SCOPE_TABLE", "");
        CassandraColumn<Integer> entrySOURCEDT = new CassandraColumn<>("SOURCE_DATA_TYPE", 0);
        CassandraColumn<String> entryISAutoIncrement = new CassandraColumn<>("IS_AUTOINCREMENT", "NO");
        CassandraColumn<String> entryISGeneratedColumn = new CassandraColumn<>("IS_GENERATEDCOLUMN", "NO");

        int ordinalPosition = 1;

        // enumerate available columns and build SQL meta-data
        for (ColumnMetadata column : table.getColumns()) {

            if (hasPatternMatch(column.getName(), columnNamePattern)) {

                CassandraColumn<String> entrySchema = new CassandraColumn<>("TABLE_SCHEM", table.getKeyspace().getName());
                CassandraColumn<String> entryTableName = new CassandraColumn<>("TABLE_NAME", table.getName());
                CassandraColumn<String> entryColumnName = new CassandraColumn<>("COLUMN_NAME", column.getName());

                // identify the validator for this datatype
                CassandraValidatorType validatorType = CassandraValidatorType.fromValidator(column.getType());

                CassandraColumn<Integer> entryDataType = new CassandraColumn<>("DATA_TYPE", validatorType.getSqlType());
                CassandraColumn<String> entryTypeName = new CassandraColumn<>("TYPE_NAME", validatorType.getSqlDisplayName());

                CassandraColumn<Integer> entryColumnSize = new CassandraColumn<>("COLUMN_SIZE", validatorType.getSqlWidth());
                CassandraColumn<Integer> entryDecimalDigits = new CassandraColumn<>("DECIMAL_DIGITS", 0x00);
                CassandraColumn<Integer> entryNPR = new CassandraColumn<>("NUM_PREC_RADIX", validatorType.getSqlRadix());
                CassandraColumn<Integer> entryCharOctetLength = new CassandraColumn<>("CHAR_OCTET_LENGTH", validatorType.getSqlLength());

                CassandraColumn<Integer> entryOrdinalPosition = new CassandraColumn<>("ORDINAL_POSITION", ordinalPosition++);
                CassandraColumn<Integer> entryNullable = new CassandraColumn<>("NULLABLE", DatabaseMetaData.columnNullable);
                CassandraColumn<String> entryISNullable = new CassandraColumn<>("IS_NULLABLE", "YES");

                CassandraRow row = new CassandraRow(
                        entryCatalog,
                        entrySchema,
                        entryTableName,
                        entryColumnName,
                        entryDataType,
                        entryTypeName,
                        entryColumnSize,
                        entryBufferLength,
                        entryDecimalDigits,
                        entryNPR,
                        entryNullable,
                        entryRemarks,
                        entryColumnDef,
                        entrySQLDataType,
                        entrySQLDateTimeSub,
                        entryCharOctetLength,
                        entryOrdinalPosition,
                        entryISNullable,
                        entryScopeCatalog,
                        entryScopeSchema,
                        entryScopeTable,
                        entrySOURCEDT,
                        entryISAutoIncrement,
                        entryISGeneratedColumn);

                // determine if this is a pkey
                if (pks.contains(column.getName())) {
                    metaResults.addFirst(row);
                } else {
                    metaResults.addRow(row);
                }
            }

        }

    }

    private static List<TableMetadata> findTableMetaData(KeyspaceMetadata keyspace,
                                                         String schemaPattern,
                                                         String tableNamePattern) {

        final List<TableMetadata> tableMetadatas = new LinkedList<>();

        if (hasPatternMatch(keyspace.getName(), schemaPattern)) {

            for (TableMetadata table : keyspace.getTables()) {
                if (hasPatternMatch(table.getName(), tableNamePattern)) {
                    tableMetadatas.add(table);
                }
            }

        }

        return tableMetadatas;

    }

    /**
     * Determine if a pattern match can be made on a specified value. This supports primitive wildcarding
     * via SQL {@code %} (percent) match conditions.
     *
     * @param value     Value to wildcard
     * @param pattern   Pattern to check
     * @return Returns {@code true} if pattern match was successful and {@code false} if not.
     */
    private static boolean hasPatternMatch(String value, String pattern) {

        // always true if no pattern requested
        if (StringUtils.isEmpty(pattern)) {
            return true;
        }

        // does pattern contain any wildcards? implement primitive like comparison
        if (StringUtils.contains(pattern, "%")) {
            String substring = StringUtils.remove(pattern, '%');
            return StringUtils.contains(value, substring);
        }

        // finally - exact match was requested
        return StringUtils.equalsIgnoreCase(value, pattern);

    }

    private static List<MaterializedViewMetadata> findViewMetaData(KeyspaceMetadata keyspace,
                                                                   String schemaPattern,
                                                                   String tableNamePattern) {

        final List<MaterializedViewMetadata> viewMetadatas = new LinkedList<>();

        if (hasPatternMatch(keyspace.getName(), schemaPattern)) {

            for (MaterializedViewMetadata view : keyspace.getMaterializedViews()) {

                if (hasPatternMatch(view.getName(), tableNamePattern)) {
                    viewMetadatas.add(view);
                }
            }

        }

        return viewMetadatas;

    }

    /**
     * Query for indexes from Cassandra. These are essentially the primary keys both clustered and non-clustered.
     *
     * @param statement Statement from which the Connection should be derived.
     * @param schema    Schema to query (keyspace).
     * @param table     Table to query (column group).
     * @return Set of unique indexes for the specified table (column group)
     * @throws SQLException Database error during index query.
     */
    public static ResultSet makeIndexes(CassandraStatement statement, String schema, String table) throws SQLException {
        //1.TABLE_CAT String => table catalog (may be null)
        //2.TABLE_SCHEM String => table schema (may be null)
        //3.TABLE_NAME String => table name
        //4.NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
        //5.INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
        //6.INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
        //7.TYPE short => index type: - tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
        //- tableIndexClustered - this is a clustered index
        //- tableIndexHashed - this is a hashed index
        //- tableIndexOther - this is some other style of index
        //
        //8.ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
        //9.COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
        //10.ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
        //11.CARDINALITY int => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.
        //12.PAGES int => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.
        //13.FILTER_CONDITION String => Filter condition, if any. (may be null)

        Cluster cluster = statement.connection.getCluster();
        List<KeyspaceMetadata> keyspaces = new LinkedList<>();

        if (StringUtils.isEmpty(schema)) {
            keyspaces.addAll(cluster.getMetadata().getKeyspaces());
        } else {
            KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(schema);

            if (keyspace != null) {
                keyspaces.add(keyspace);
            }
        }

        // resultset for return
        MetadataResultSets metaResults = new MetadataResultSets();

        // loop over the keyspaces looking for primary key data
        for (KeyspaceMetadata keyspace : keyspaces) {

            for (TableMetadata tableMetadata : findTableMetaData(keyspace, schema, table)) {
                appendIndexCassandraRow(cluster, keyspace, tableMetadata, metaResults);
            }

        }

        return metaResults;

    }

    public boolean next() throws SQLException {
        return (++rowId < rows.size());
    }

    public void close() throws SQLException {
        rowId = 0;
        rows.clear();
    }

    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    public String getString(int columnIndex) throws SQLException {
        Object value = rows.get(getRow()).getColumnValue(columnIndex - 1);

        if (value instanceof Integer) {
            return ((Integer) value).toString();
        } else if (value instanceof Long) {
            return ((Long) value).toString();
        } else if (value instanceof Double) {
            return ((Double) value).toString();
        } else if (value instanceof Float) {
            return ((Float) value).toString();
        } else if (value instanceof String) {
            return ((String) value);
        } else {
            return value.getClass().getName();
        }
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    public short getShort(int columnIndex) throws SQLException {
        return (short) getInt(columnIndex);
    }

    public int getInt(int columnIndex) throws SQLException {
        Integer i = rows.get(getRow()).getColumnValue(columnIndex - 1);
        return i.shortValue();
    }

    public long getLong(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public float getFloat(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    public Date getDate(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return new CassandraResultSetMetaData(this);
    }

    public Object getObject(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public Object getObject(String columnLabel) throws SQLException {
        return rows.get(getRow()).getColumnValue(findColumn(columnLabel));
    }

    public int findColumn(String columnLabel) throws SQLException {

        if (rows.size() > 0) {
            return rows.get(0).findColumnId(columnLabel);
        } else {
            throw new SQLException("Empty resultset");
        }

    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    public boolean isBeforeFirst() throws SQLException {
        return (-1 == rowId);
    }

    public boolean isAfterLast() throws SQLException {
        return (rowId > rows.size());
    }

    public boolean isFirst() throws SQLException {
        return (rowId <= 0);
    }

    public boolean isLast() throws SQLException {
        return (rowId > rows.size());
    }

    public void beforeFirst() throws SQLException {
        rowId = -1;
    }

    public void afterLast() throws SQLException {
        rowId = (rows.size() + 1);
    }

    public boolean first() throws SQLException {
        rowId = 0;
        return true;
    }

    public boolean last() throws SQLException {
        rowId = rows.size();
        return true;
    }

    public int getRow() throws SQLException {
        return rowId <= 0 ? 0 : rowId;
    }

    public boolean absolute(int row) throws SQLException {
        if (row < rows.size()) {
            rowId = row;
            return true;
        } else {
            return false;
        }
    }

    public boolean relative(int rows) throws SQLException {
        if ((rowId + rows) < this.rows.size()) {
            rowId += rows;
            return true;
        } else {
            return false;
        }
    }

    public boolean previous() throws SQLException {

        if (rowId > 0) {
            rowId--;
            return true;
        } else {
            return false;
        }
    }

    public void setFetchDirection(int direction) throws SQLException {

    }

    public int getFetchDirection() throws SQLException {
        return 0;
    }

    public void setFetchSize(int rows) throws SQLException {

    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getType() throws SQLException {
        return 0;
    }

    public int getConcurrency() throws SQLException {
        return 0;
    }

    public Statement getStatement() throws SQLException {
        return null;
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        return new RowId() {
            public byte[] getBytes() {
                return new byte[rowId];
            }
        };
    }

    public int getHoldability() throws SQLException {
        return 0;
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return rows.get(getRow()).getColumnValue(findColumn(columnLabel));
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // TODO: move into the meta package and rescope
    public int getColumnCount() {

        if (this.rows.size() > 0) {
            CassandraRow row = rows.get(0);
            return row.getColumnCount();
        } else {
            return 0;
        }
    }

    public String getName(int column) throws SQLException {

        if (!this.rows.isEmpty()) {
            return this.rows.get(0).getColumnName(column);
        } else {
            return null;
        }

    }

    /**
     * Construct the primary keys for the specified schema and table name.
     *
     * @param statement Statement to use for the Connection
     * @param schema    Schema (keyspace) for the primary keys request
     * @param table     Table (column group) for the keys request.
     * @return Set of primary keys in standard format.
     * @throws SQLException Database error.
     */
    public static ResultSet makePrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException {
        //1.TABLE_CAT String => table catalog (may be null)
        //2.TABLE_SCHEM String => table schema (may be null)
        //3.TABLE_NAME String => table name
        //4.COLUMN_NAME String => column name
        //5.KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
        //6.PK_NAME String => primary key name (may be null)

        final Cluster cluster = statement.connection.getCluster();

        // resulting metadata
        final MetadataResultSets metaResults = new MetadataResultSets();

        KeyspaceMetadata keyspace = cluster.getMetadata().getKeyspace(schema);

        if (null != keyspace) {

            // match on table
            for (TableMetadata tableMetadata : findTableMetaData(keyspace, schema, table)) {
                appendKeyCassandraRow(cluster, keyspace, tableMetadata, metaResults);
            }

            // match on view
            for (MaterializedViewMetadata viewMetadata : findViewMetaData(keyspace, schema, table)) {
                appendKeyCassandraRow(cluster, keyspace, viewMetadata, metaResults);
            }

        }

        return metaResults;

    }

    private static void appendKeyCassandraRow(final Cluster cluster,
                                              final KeyspaceMetadata keyspace,
                                              final AbstractTableMetadata tableMetadata,
                                              final MetadataResultSets metaResults) {

        int ordinalPosition = 1;

        CassandraColumn<String> entryCatalog = new CassandraColumn<>("TABLE_CAT", cluster.getClusterName());
        CassandraColumn<String> entrySchema = new CassandraColumn<>("TABLE_SCHEM", keyspace.getName());
        CassandraColumn<String> entryTableName = new CassandraColumn<>("TABLE_NAME", tableMetadata.getName());

        for (ColumnMetadata columnMetadata : tableMetadata.getPrimaryKey()) {

            CassandraColumn<String> entryColumnName = new CassandraColumn<>("COLUMN_NAME", columnMetadata.getName());
            CassandraColumn<Integer> entryKeySeq = new CassandraColumn<>("KEY_SEQ", ordinalPosition++);
            CassandraColumn<String> entryPKName = new CassandraColumn<>("PK_NAME", "");

            CassandraRow row = new CassandraRow(
                    entryCatalog,
                    entrySchema,
                    entryTableName,
                    entryColumnName,
                    entryKeySeq,
                    entryPKName
            );

            metaResults.addRow(row);

        }

    }

    private static void appendIndexCassandraRow(final Cluster cluster,
                                                final KeyspaceMetadata keyspace,
                                                final TableMetadata tableMetadata,
                                                final MetadataResultSets metaResults) {

        int ordinalPosition = 1;

        for (IndexMetadata indexMetadata : tableMetadata.getIndexes()) {

            CassandraColumn<String> entryCatalog = new CassandraColumn<>("TABLE_CAT", cluster.getClusterName());
            CassandraColumn<String> entrySchema = new CassandraColumn<>("TABLE_SCHEM", keyspace.getName());
            CassandraColumn<String> entryTableName = new CassandraColumn<>("TABLE_NAME", tableMetadata.getName());

            CassandraColumn<Boolean> entryNonUnique = new CassandraColumn<>("NON_UNIQUE", Boolean.TRUE);
            CassandraColumn<String> entryIndexQualifier = new CassandraColumn<>("INDEX_QUALIFIER", keyspace.getName());

            CassandraColumn<String> entryIndexName = new CassandraColumn<>("INDEX_NAME", indexMetadata.getName());

            CassandraColumn<Short> entryType = new CassandraColumn<>("TYPE", DatabaseMetaData.tableIndexHashed);
            CassandraColumn<Integer> entryOrdinalPosition = new CassandraColumn<>("ORDINAL_POSITION", ordinalPosition++);

            CassandraColumn<String> entryColumnName = new CassandraColumn<>("COLUMN_NAME", indexMetadata.getTarget());
            CassandraColumn<String> entryAoD = new CassandraColumn<>("ASC_OR_DESC", null);
            CassandraColumn<Integer> entryCardinality = new CassandraColumn<>("CARDINALITY", -1);
            CassandraColumn<Integer> entryPages = new CassandraColumn<>("PAGES", -1);
            CassandraColumn<String> entryFilter = new CassandraColumn<>("FILTER_CONDITION", "");

            CassandraRow row = new CassandraRow(
                    entryCatalog,
                    entrySchema,
                    entryTableName,
                    entryNonUnique,
                    entryIndexQualifier,
                    entryIndexName,
                    entryType,
                    entryOrdinalPosition,
                    entryColumnName,
                    entryAoD,
                    entryCardinality,
                    entryPages,
                    entryFilter
            );

            metaResults.addRow(row);

        }

    }

}