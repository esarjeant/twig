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

import com.micromux.cassandra.jdbc.meta.CassandraColumn;
import com.micromux.cassandra.jdbc.meta.CassandraResultSetMetaData;
import com.micromux.cassandra.jdbc.meta.CassandraRow;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class MetadataResultSets extends AbstractResultSet implements ResultSet
{
    static final String TABLE_CONSTANT = "TABLE";

    // array backed results - navigate using rowId
    private final List<CassandraRow> rows = new ArrayList<CassandraRow>();

    // current row id
    private int rowId = -1;

    // was the last value null?
    private boolean wasNull = false;

    // Private Constructor
    private MetadataResultSets() {}

    /**
     * Add a new row to the resultset.
     * @param row   Row to add to the resultset.
     */
    void addRow(CassandraRow row) {
        this.rows.add(row);
    }

    /**
     * Add a new row to the resultset as one of the first records. This is used
     * for primary key columns.
     * @param row   Row to add first to the resultset.
     */
    void addFirst(CassandraRow row) {
        this.rows.add(0, row);
    }

    /**
     * The table types available in this database.
     * @param statement  Statement that needs meta-data.
     * @return The resultset containing the available table types.
     * @throws SQLException  Fatal database error.
     */
    public static ResultSet makeTableTypes(CassandraStatement statement) throws SQLException
    {
        //final  Entry[][] tableTypes = { { new Entry("TABLE_TYPE",bytes(TABLE_CONSTANT),Entry.ASCII_TYPE)} };

        // use tableTypes with the key in column number 1 (one based)
        // CassandraResultSet result =  makeCqlResult(tableTypes, 1);

        MetadataResultSets metaResults = new MetadataResultSets();

        CassandraColumn<String> tableTypeCol = new CassandraColumn<String>("TABLE_TYPE", TABLE_CONSTANT);
        CassandraRow row = new CassandraRow(tableTypeCol);

        metaResults.addRow(row);

        return metaResults;

    }

    public static ResultSet makeCatalogs(CassandraStatement statement) throws SQLException
    {

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
     *
     * <P>The schema columns are:
     *  <OL>
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
     *  <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be <code>null</code>)
     *  </OL>
     *
     * @return a <code>ResultSet</code> object in which each row is a
     *         schema description
     * @exception SQLException if a database access error occurs
     *
     */
    public static ResultSet makeSchemas(CassandraStatement statement, String schemaPattern) throws SQLException
    {
        if ("%".equals(schemaPattern)) schemaPattern = null;

        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)

        String query = "SELECT keyspace_name FROM system.schema_keyspaces";
        if (schemaPattern!=null) query = query + " where keyspace_name = '" + schemaPattern + "'";

        String catalog = statement.connection.getCatalog();
        CassandraColumn<String> entryCatalog = new CassandraColumn<String>("TABLE_CATALOG", catalog);

        // determine the schemas
        CassandraResultSet result = (CassandraResultSet)statement.executeQuery(query);

        // create meta resultset
        MetadataResultSets metaResult = new MetadataResultSets();

        while (result.next())
        {
            CassandraColumn<String> entrySchema = new CassandraColumn<String>("TABLE_SCHEM", result.getString(1));

            CassandraRow row = new CassandraRow(entrySchema, entryCatalog);
            metaResult.addRow(row);

        }

        return metaResult;

    }

    /**
     * Query the list of available tables in the schema. This translates to column groups in Cassandra.
     * This must match for the format expected by JDBC.
     *
     * @param statement        Statement to use for query. This identifies the session to be used to execute CQL.
     * @param schemaPattern    Pattern to match on; this may be {@code null}.
     * @param tableNamePattern Pattern to match on; this may be {@code null}.
     * @return Navigable resultset that can be used to review the results of the table query.
     * @throws SQLException  Fatal database error.
     */
    public static ResultSet makeTables(CassandraStatement statement, String schemaPattern, String tableNamePattern) throws SQLException
    {
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

        if ("%".equals(schemaPattern)) schemaPattern = null;
        if ("%".equals(tableNamePattern)) tableNamePattern = null;

        // example query to retrieve tables
        // SELECT keyspace_name,columnfamily_name,comment from schema_columnfamilies WHERE columnfamily_name = 'Test2';
        StringBuilder query = new StringBuilder("SELECT keyspace_name,columnfamily_name,comment FROM system.schema_columnfamilies");

        int filterCount = 0;
        if (schemaPattern != null) filterCount++;
        if (tableNamePattern != null) filterCount++;

        // check to see if it is qualified
        if (filterCount > 0)
        {
            String expr = "%s = '%s'";
            query.append(" WHERE ");
            if (schemaPattern != null)
            {
            	query.append(String.format(expr, "keyspace_name", schemaPattern));
                filterCount--;
                if (filterCount > 0) query.append(" AND ");
            }
            if (tableNamePattern != null) query.append(String.format(expr, "columnfamily_name", tableNamePattern));
            query.append(" ALLOW FILTERING");
        }

        String catalog = statement.connection.getCatalog();
        CassandraColumn<String> entryCatalog = new CassandraColumn<String>("TABLE_CAT", catalog);
        CassandraColumn<String> entryTableType = new CassandraColumn<String>("TABLE_TYPE", TABLE_CONSTANT);
        CassandraColumn<String> entryTypeCatalog = new CassandraColumn<String>("TYPE_CAT", "");
        CassandraColumn<String> entryTypeSchema = new CassandraColumn<String>("TYPE_SCHEM", "");
        CassandraColumn<String> entryTypeName = new CassandraColumn<String>("TYPE_NAME", "");
        CassandraColumn<String> entrySRCN = new CassandraColumn<String>("SELF_REFERENCING_COL_NAME", "");
        CassandraColumn<String> entryRefGeneration = new CassandraColumn<String>("REF_GENERATION", "");

        // determine the schemas
        CassandraResultSet result = (CassandraResultSet)statement.executeQuery(query.toString());

        // create returned resultset
        MetadataResultSets metaResults = new MetadataResultSets();

        while (result.next())
        {
            CassandraColumn<String> entrySchema = new CassandraColumn<String>("TABLE_SCHEM", result.getString(1));
            CassandraColumn<String> entryTableName = new CassandraColumn<String>("TABLE_NAME",
                (result.getString(2) == null) ? "" : result.getString(2));
            CassandraColumn<String> entryRemarks = new CassandraColumn<String>("REMARKS",
                (result.getString(3) == null) ? "" : result.getString(3));

            CassandraRow row = new CassandraRow(entryCatalog,
                    entrySchema,
                    entryTableName,
                    entryTableType,
                    entryRemarks,
                    entryTypeCatalog,
                    entryTypeSchema,
                    entryTypeName,
                    entrySRCN,
                    entryRefGeneration);

            metaResults.addRow(row);

        }

        return metaResults;

    }

    /**
     * Query the column schema for the specified table name pattern.
     * @param statement         Valid statement with Connection.
     * @param schemaPattern     Optional schema pattern or {@code null}
     * @param tableNamePattern  Optional table name pattern or {@code null} for all.
     * @param columnNamePattern Optional column name pattern or {@code null} for all.
     * @return Results with column information.
     * @throws SQLException
     */
    public static ResultSet makeColumns(CassandraStatement statement, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException, CharacterCodingException {
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

        if ("%".equals(schemaPattern)) schemaPattern = null;
        if ("%".equals(tableNamePattern)) tableNamePattern = null;
        if ("%".equals(columnNamePattern)) columnNamePattern = null;

        StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, validator FROM system.schema_columns");


        int filterCount = 0;
        if (schemaPattern != null) filterCount++;
        if (tableNamePattern != null) filterCount++;
        if (columnNamePattern != null) filterCount++;

        // check to see if it is qualified
        if (filterCount > 0)
        {
            String expr = "%s = '%s'";
            query.append(" WHERE ");
            if (schemaPattern != null)
            {
            	query.append(String.format(expr, "keyspace_name", schemaPattern));
                filterCount--;
                if (filterCount > 0) query.append(" AND ");
            }
            if (tableNamePattern != null)
            {
            	query.append(String.format(expr, "columnfamily_name", tableNamePattern));
                filterCount--;
                if (filterCount > 0) query.append(" AND ");
            }
            if (columnNamePattern != null) query.append(String.format(expr, "column_name", columnNamePattern));
            query.append(" ALLOW FILTERING");
        }

        String catalog = statement.connection.getCatalog();
        CassandraColumn<String> entryCatalog = new CassandraColumn<String>("TABLE_CAT", catalog);
        CassandraColumn<Integer> entryBufferLength = new CassandraColumn<Integer>("BUFFER_LENGTH", 0);
        CassandraColumn<String> entryRemarks = new CassandraColumn<String>("REMARKS", "");
        CassandraColumn<String> entryColumnDef = new CassandraColumn<String>("COLUMN_DEF", "");
        CassandraColumn<String> entrySQLDataType = new CassandraColumn<String>("SQL_DATA_TYPE", "");
        CassandraColumn<Integer> entrySQLDateTimeSub = new CassandraColumn<Integer>("SQL_DATETIME_SUB", 0);
        CassandraColumn<String> entryScopeCatalog = new CassandraColumn<String>("SCOPE_CATLOG", "");
        CassandraColumn<String> entryScopeSchema = new CassandraColumn<String>("SCOPE_SCHEMA", "");
        CassandraColumn<String> entryScopeTable = new CassandraColumn<String>("SCOPE_TABLE", "");
        CassandraColumn<Integer> entrySOURCEDT = new CassandraColumn<Integer>("SOURCE_DATA_TYPE", 0);
        CassandraColumn<String> entryISAutoIncrement = new CassandraColumn<String>("IS_AUTOINCREMENT", "NO");
        CassandraColumn<String> entryISGeneratedColumn = new CassandraColumn<String>("IS_GENERATEDCOLUMN", "NO");

        int ordinalPosition = 0;

        // define the PK columns
		List<PKInfo> pks = getPrimaryKeys(statement, schemaPattern, tableNamePattern);

        // create the resultsets that will return...
        MetadataResultSets metaResults = new MetadataResultSets();

        // define the columns
        CassandraResultSet result = (CassandraResultSet) statement.executeQuery(query.toString());
        while (result.next())
        {
            CassandraColumn<String> entrySchema = new CassandraColumn<String>("TABLE_SCHEM", result.getString(1));
            CassandraColumn<String> entryTableName = new CassandraColumn<String>("TABLE_NAME",
                (result.getString(2) == null) ? "" : result.getString(2));
            CassandraColumn<String> entryColumnName = new CassandraColumn<String>("COLUMN_NAME",
                (result.getString(3) == null) ? "" : result.getString(3));

            String validator = result.getString(4);
            CassandraValidatorType validatorType = CassandraValidatorType.fromValidator(validator);

            CassandraColumn<Integer> entryDataType = new CassandraColumn<Integer>("DATA_TYPE", validatorType.getSqlType());
            CassandraColumn<String> entryTypeName = new CassandraColumn<String>("TYPE_NAME", validatorType.getSqlDisplayName());

            CassandraColumn<Integer> entryColumnSize = new CassandraColumn<Integer>("COLUMN_SIZE", validatorType.getSqlWidth());
            CassandraColumn<Integer> entryDecimalDigits = new CassandraColumn<Integer>("DECIMAL_DIGITS", 0x00);
            CassandraColumn<Integer> entryNPR = new CassandraColumn<Integer>("NUM_PREC_RADIX", validatorType.getSqlRadix());
            CassandraColumn<Integer> entryCharOctetLength = new CassandraColumn<Integer>("CHAR_OCTET_LENGTH", validatorType.getSqlLength());

            ordinalPosition++;
            CassandraColumn<Integer> entryOrdinalPosition = new CassandraColumn<Integer>("ORDINAL_POSITION", ordinalPosition);
            CassandraColumn<Integer> entryNullable = new CassandraColumn<Integer>("NULLABLE", DatabaseMetaData.columnNullable);
            CassandraColumn<String> entryISNullable = new CassandraColumn<String>("IS_NULLABLE", "YES");

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
                    entryISGeneratedColumn
            );

            // determine if this is a pkey
            if (isPrimaryKeyColumn(pks, entryColumnName)) {
                metaResults.addFirst(row);
            } else {
                metaResults.addRow(row);
            }

        }

        return metaResults;

    }

    private static boolean isPrimaryKeyColumn(List<PKInfo> pks, CassandraColumn<String> entryColumnName) {

        for (PKInfo i : pks) {
            if (entryColumnName.getValue().equalsIgnoreCase(i.name)) {
                return true;
            }
        }

        return false;

    }

    /**
     * Query for indexes from Cassandra. These are essentially the primary keys both clustered and non-clustered.
     * @param statement     Statement from which the Connection should be derived.
     * @param schema        Schema to query (keyspace).
     * @param table         Table to query (column group).
     * @return Set of unique indexes for the specified table (column group)
     * @throws SQLException Database error during index query.
     */
    public static ResultSet makeIndexes(CassandraStatement statement, String schema, String table) throws SQLException
	{
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

	    StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, component_index, index_name, index_options, index_type FROM system.schema_columns");

	    int filterCount = 0;
	    if (schema != null) filterCount++;
	    if (table != null) filterCount++;

	    // check to see if it is qualified
	    if (filterCount > 0)
	    {
	        String expr = "%s = '%s'";
	        query.append(" WHERE ");
	        if (schema != null)
	        {
	        	query.append(String.format(expr, "keyspace_name", schema));
                filterCount--;
		        if (filterCount > 0) query.append(" AND ");
	        }
	        if (table != null) query.append(String.format(expr, "columnfamily_name", table));
	        query.append(" ALLOW FILTERING");
	    }

	    // name of the catalog (keyspace)
	    String catalog = statement.connection.getCatalog();
	    CassandraColumn<String> entryCatalog = new CassandraColumn<String>("TABLE_CAT", catalog);

        // resultset for return
	    MetadataResultSets metaResults = new MetadataResultSets();

        int ordinalPosition = 0;
	    // define the columns
        CassandraResultSet result = (CassandraResultSet) statement.executeQuery(query.toString());
	    while (result.next())
	    {
	        if (result.getString(7) == null) continue; //if there is no index_type its not an index
	        ordinalPosition++;

	        CassandraColumn<String> entrySchema = new CassandraColumn<String>("TABLE_SCHEM", result.getString(1));
	        CassandraColumn<String> entryTableName = new CassandraColumn<String>("TABLE_NAME",
	            (result.getString(2) == null) ? "" : result.getString(2));
	        CassandraColumn<Boolean> entryNonUnique = new CassandraColumn<Boolean>("NON_UNIQUE", Boolean.TRUE);
	        CassandraColumn<String>  entryIndexQualifier = new CassandraColumn<String>("INDEX_QUALIFIER", catalog);
            CassandraColumn<String> entryIndexName = new CassandraColumn<String>("INDEX_NAME", (result.getString(5) == null ? "" : result.getString(5)));
            CassandraColumn<Short> entryType = new CassandraColumn<Short>("TYPE", DatabaseMetaData.tableIndexHashed);
            CassandraColumn<Integer> entryOrdinalPosition = new CassandraColumn<Integer>("ORDINAL_POSITION", ordinalPosition);
            CassandraColumn<String> entryColumnName = new CassandraColumn<String>("COLUMN_NAME",
                    (result.getString(3) == null) ? "" : result.getString(3));
            CassandraColumn<String> entryAoD = new CassandraColumn<String>("ASC_OR_DESC", "");
            CassandraColumn<Integer> entryCardinality = new CassandraColumn<Integer>("CARDINALITY", -1);
            CassandraColumn<Integer> entryPages = new CassandraColumn<Integer>("PAGES", -1);
            CassandraColumn<String> entryFilter = new CassandraColumn<String>("FILTER_CONDITION", "");

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

        return metaResults;

	}

	private static List<PKInfo> getPrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException
	{
		StringBuilder query = new StringBuilder("SELECT keyspace_name, columnfamily_name, column_name, validator, type FROM system.schema_columns");

	    int filterCount = 0;
	    if (schema != null) filterCount++;
	    if (table != null) filterCount++;

	    // check to see if it is qualified
	    if (filterCount > 0)
	    {
	        String expr = "%s = '%s'";
	        query.append(" WHERE ");
	        if (schema != null)
	        {
	        	query.append(String.format(expr, "keyspace_name", schema));
                filterCount--;
		        if (filterCount > 0) query.append(" AND ");
	        }
	        if (table != null) query.append(String.format(expr, "columnfamily_name", table));
	        query.append(" ALLOW FILTERING");
	    }

        // interrogate the results forp primary keys
	    List<PKInfo> primaryKeys = new ArrayList<PKInfo>();
	    CassandraResultSet result = (CassandraResultSet) statement.executeQuery(query.toString());
	    while (result.next())
	    {

	    	String rschema = result.getString(1);
   	    	String rtable = result.getString(2);
	        String columnName = result.getString(3);
            String validator = result.getString(4);
            String type = result.getString(5);

            if (!"regular".equalsIgnoreCase(type)) {

                CassandraValidatorType validatorType = CassandraValidatorType.fromValidator(validator);

                PKInfo pk = createPKInfo(primaryKeys, rschema, rtable, columnName, validatorType);
                primaryKeys.add(pk);

            }

	    }

	    return primaryKeys;

	}

	private static PKInfo createPKInfo(List<PKInfo> retval, String schema, String table, String columnName, CassandraValidatorType validator)
	{
        PKInfo pki = new PKInfo();
        pki.name = columnName;
        pki.schema = schema;
        pki.table = table;
        pki.type = validator.getSqlType();
        pki.typeName = validator.getSqlName();

        return pki;

	}

    @Override
    public boolean next() throws SQLException {
        return (++rowId < rows.size());
    }

    @Override
    public void close() throws SQLException {
        rowId = 0;
        rows.clear();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = rows.get(getRow()).getColumnValue(columnIndex - 1);

        if (value instanceof Integer) {
            return ((Integer)value).toString();
        } else if (value instanceof Long) {
            return ((Long)value).toString();
        } else if (value instanceof Double) {
            return ((Double)value).toString();
        } else if (value instanceof Float) {
            return ((Float)value).toString();
        } else if (value instanceof String) {
            return ((String)value);
        } else {
            return value.getClass().getName();
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short)getInt(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Integer i = rows.get(getRow()).getColumnValue(columnIndex - 1);
        return i.shortValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new CassandraResultSetMetaData(this);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return rows.get(getRow()).getColumnValue(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {

        if (rows.size() > 0) {
            return rows.get(0).findColumnId(columnLabel);
        } else {
            throw new SQLException("Empty resultset");
        }

    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return (-1 == rowId);
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return (rowId > rows.size());
    }

    @Override
    public boolean isFirst() throws SQLException {
        return (rowId <= 0);
    }

    @Override
    public boolean isLast() throws SQLException {
        return (rowId > rows.size());
    }

    @Override
    public void beforeFirst() throws SQLException {
        rowId = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        rowId = (rows.size() + 1);
    }

    @Override
    public boolean first() throws SQLException {
        rowId = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        rowId = rows.size();
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        return rowId <= 0 ? 0 : rowId;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (row < rows.size()) {
            rowId = row;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if ((rowId + rows) < this.rows.size()) {
            rowId += rows;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {

        if (rowId > 0) {
            rowId--;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return new RowId() {
            @Override
            public byte[] getBytes() {
                return new byte[rowId];
            }
        };
    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return rows.get(getRow()).getColumnValue(columnIndex - 1);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return rows.get(getRow()).getColumnValue(findColumn(columnLabel));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
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

    private static class PKInfo
	{
		public String typeName;
		public String schema;
		public String table;
		public String name;
		public int type;
	}

    /**
     * Construct the primary keys for the specified schema and table name.
     * @param statement     Statement to use for the Connection
     * @param schema        Schema (keyspace) for the primary keys request
     * @param table         Table (column group) for the keys request.
     * @return Set of primary keys in standard format.
     * @throws SQLException  Database error.
     */
	public static ResultSet makePrimaryKeys(CassandraStatement statement, String schema, String table) throws SQLException
	{
		//1.TABLE_CAT String => table catalog (may be null)
		//2.TABLE_SCHEM String => table schema (may be null)
		//3.TABLE_NAME String => table name
		//4.COLUMN_NAME String => column name
		//5.KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
		//6.PK_NAME String => primary key name (may be null)

		List<PKInfo> pks = getPrimaryKeys(statement, schema, table);

	    String catalog = statement.connection.getCatalog();
        CassandraColumn<String> entryCatalog = new CassandraColumn<String>("TABLE_CAT", catalog);

        int seq = 0;

        // resulting metadata
        MetadataResultSets metaResults = new MetadataResultSets();

	    // define the columns
        for (PKInfo info : pks)
        {

	        CassandraColumn<String> entrySchema = new CassandraColumn<String>("TABLE_SCHEM", info.schema);
            CassandraColumn<String> entryTableName = new CassandraColumn<String>("TABLE_NAME", info.table);
            CassandraColumn<String> entryColumnName = new CassandraColumn<String>("COLUMN_NAME", info.name);

            seq++;
            CassandraColumn<Integer> entryKeySeq = new CassandraColumn<Integer>("KEY_SEQ", seq);
            CassandraColumn<String> entryPKName = new CassandraColumn<String>("PK_NAME", "");

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

        return metaResults;

	}

}