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
package org.apache.cassandra.cql.jdbc;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.ByteBufferUtil.string;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;



public  class MetadataResultSets
{
    
    private static final String UTF8_TYPE = "UTF8Type";
    private static final String ASCII_TYPE = "AsciiType";

    public static final MetadataResultSets instance = new MetadataResultSets();
    
    
    /**
     * Make a {@code Column} from a column name and a value.
     * 
     * @param name
     *          the name of the column
     * @param value
     *          the value of the column as a {@code ByteBufffer}
     * 
     * @return {@code Column}
     */
    private static final Column makeColumn(String name, ByteBuffer value)
    {
      return new Column(bytes(name)).setValue(value).setTimestamp(System.currentTimeMillis());
    }

    private static final CqlRow makeRow(String key, List<Column> columnList)
    {
      return new CqlRow(bytes(key), columnList);
    }
    
    private static CqlMetadata makeMetadataAllString(List<String> colNameList)
    {
        Map<ByteBuffer,String> namesMap = new HashMap<ByteBuffer,String>();
        Map<ByteBuffer,String> valuesMap = new HashMap<ByteBuffer,String>();
        
        for (String name : colNameList)
        {
            namesMap.put(bytes(name), ASCII_TYPE);
            valuesMap.put(bytes(name), UTF8_TYPE);
        }
        
        return new CqlMetadata(namesMap,valuesMap,ASCII_TYPE,UTF8_TYPE);
    }

    private static CqlMetadata makeMetadata(List<Entry> entries)
    {
        Map<ByteBuffer,String> namesMap = new HashMap<ByteBuffer,String>();
        Map<ByteBuffer,String> valuesMap = new HashMap<ByteBuffer,String>();
        
        for (Entry entry : entries)
        {
            namesMap.put(bytes(entry.name), ASCII_TYPE);
            valuesMap.put(bytes(entry.name), entry.type);
        }
        
        return new CqlMetadata(namesMap,valuesMap,ASCII_TYPE,UTF8_TYPE);
    }


    private CqlResult makeCqlResult(Entry[][] rowsOfcolsOfKvps, int position)
    {
        CqlResult result = new CqlResult(CqlResultType.ROWS);
        CqlMetadata meta = null;
        CqlRow row = null;
        Column column = null;
        List<Column> columnlist = new LinkedList<Column>();
        List<CqlRow> rowlist = new LinkedList<CqlRow>();
        List<String> colNamesList = new ArrayList<String>();
        
        
        for (int rowcnt = 0; rowcnt < rowsOfcolsOfKvps.length; rowcnt++ )
        {
            colNamesList = new ArrayList<String>();
            columnlist = new LinkedList<Column>();
            for (int colcnt = 0; colcnt < rowsOfcolsOfKvps[0].length; colcnt++ )
            {
                column = makeColumn(rowsOfcolsOfKvps[rowcnt][colcnt].name,rowsOfcolsOfKvps[rowcnt][colcnt].value);
                columnlist.add(column);
                colNamesList.add(rowsOfcolsOfKvps[rowcnt][colcnt].name);
            }
            row = makeRow(rowsOfcolsOfKvps[rowcnt][position-1].name,columnlist);
            rowlist.add(row);
        }
        
        meta = makeMetadataAllString(colNamesList);
        result.setSchema(meta).setRows(rowlist);
        return result;
    }
    
    private CqlResult makeCqlResult(List<List<Entry>> rows, int position) throws CharacterCodingException
    {
        CqlResult result = new CqlResult(CqlResultType.ROWS);
        CqlMetadata meta = null;
        CqlRow row = null;
        Column column = null;
        List<Column> columnlist = new LinkedList<Column>();
        List<CqlRow> rowlist = new LinkedList<CqlRow>();
        
        assert(!rows.isEmpty());
        
        for (List<Entry> aRow : rows )
        {
            columnlist = new LinkedList<Column>();
            
            assert (!aRow.isEmpty());
            
            // only need to do it once
            if (meta == null) meta = makeMetadata(aRow);
            
            for (Entry entry : aRow )
            {
                column = makeColumn(entry.name,entry.value);
                columnlist.add(column);
            }
            row = makeRow(string(columnlist.get(position-1).name),columnlist);
            rowlist.add(row);
        }
        
        result.setSchema(meta).setRows(rowlist);
        return result;
    }
 
    

    public  CassandraResultSet makeTableTypes(CassandraStatement statement) throws SQLException
    {
        final  Entry[][] tableTypes = { { new Entry("TABLE_TYPE",bytes("TABLE"),ASCII_TYPE)} };
        
        // use tableTypes with the key in column number 1 (one based)
        CqlResult cqlresult =  makeCqlResult(tableTypes, 1);
        
        CassandraResultSet result = new CassandraResultSet(statement,cqlresult);
        return result;
    }

    public  CassandraResultSet makeCatalogs(CassandraStatement statement) throws SQLException
    {
        final Entry[][] catalogs = { { new Entry("TABLE_CAT",bytes(statement.connection.cluster),ASCII_TYPE)} };

        // use catalogs with the key in column number 1 (one based)
        CqlResult cqlresult =  makeCqlResult(catalogs, 1);
        
        CassandraResultSet result = new CassandraResultSet(statement,cqlresult);
        return result;
    }
    
    public  CassandraResultSet makeSchemas(CassandraStatement statement, String schemaPattern) throws SQLException
    {

        // TABLE_SCHEM String => schema name
        // TABLE_CATALOG String => catalog name (may be null)
        
        String catalog = statement.connection.cluster;
        String query = "SELECT keyspace_name FROM system.schema_keyspaces";
        if (schemaPattern!=null) query = query + " where keyspace_name = '" + schemaPattern + "'";
        Entry entryC = new Entry("TABLE_CATALOG",bytes(catalog),ASCII_TYPE);
        CassandraResultSet result;
        List<Entry> col;
        List<List<Entry>> rows = new ArrayList<List<Entry>>();
        // determine the schemas
        result = (CassandraResultSet)statement.executeQuery(query);
        
        
        while (result.next())
        {
            Entry entryS = new Entry("TABLE_SCHEM",bytes(result.getString(1)),ASCII_TYPE);
            col = new ArrayList<Entry>();
            col.add(entryC);
            col.add(entryS);
            rows.add(col);
        }
        
        // just return the empty result if there were no rows
        if (rows.isEmpty() )return result;

        // use schemas with the key in column number 2 (one based)
        CqlResult cqlresult;
        try
        {
            cqlresult = makeCqlResult(rows, 1);
        }
        catch (CharacterCodingException e)
        {
            throw new SQLTransientException(e);
        }
        
        result = new CassandraResultSet(statement,cqlresult);
        return result;
    }
    
    public  CassandraResultSet makeTables(CassandraStatement statement, String schemaPattern, String tableNamePattern) throws SQLException
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

        // SELECT keyspace_name,columnfamily_name,comment from schema_columnfamilies
        //  WHERE  columnfamily_name = 'Test2';
        
        
        String catalog = statement.connection.cluster;
        
        StringBuilder query = new StringBuilder("SELECT keyspace_name,columnfamily_name,comment FROM system.schema_columnfamilies");
        String suffix1 = " WHERE %s = '%s'";
        String suffix2 = " AND %s = '%s'";
        
        // check to see if it is qualified by schemaPattern or tableNamePattern
        if      ((schemaPattern!=null) && tableNamePattern==null) query.append(String.format(suffix1, "keyspace_name", schemaPattern));
        else if ((schemaPattern==null) && tableNamePattern!=null) query.append(String.format(suffix1, "columnfamily_name", tableNamePattern));
        else if ((schemaPattern!=null) && tableNamePattern!=null)
            query.append(String.format(suffix1 + suffix2, "keyspace_name", schemaPattern,"columnfamily_name", tableNamePattern));
        System.out.println(query.toString());

        Entry entryC = new Entry("TABLE_CAT",bytes(catalog),ASCII_TYPE);
        Entry entryT = new Entry("TABLE_TYPE",bytes("TABLE"),ASCII_TYPE);
        CassandraResultSet result;
        List<Entry> col;
        List<List<Entry>> rows = new ArrayList<List<Entry>>();
        
        // determine the schemas
        result = (CassandraResultSet)statement.executeQuery(query.toString());
        
        
        while (result.next())
        {
            Entry entryS = new Entry("TABLE_SCHEM",bytes(result.getString(1)),ASCII_TYPE);
            Entry entryN = new Entry("TABLE_NAME",(result.getString(2)==null)?ByteBufferUtil.EMPTY_BYTE_BUFFER: bytes(result.getString(2)),ASCII_TYPE);
            Entry entryR = new Entry("REMARKS",(result.getString(3)==null)?ByteBufferUtil.EMPTY_BYTE_BUFFER: bytes(result.getString(3)),ASCII_TYPE);
            col = new ArrayList<Entry>();
            col.add(entryC);
            col.add(entryS);
            col.add(entryN);
            col.add(entryT);
            col.add(entryR);
            rows.add(col);
        }
        
        // just return the empty result if there were no rows
        if (rows.isEmpty() )return result;

        // use schemas with the key in column number 2 (one based)
        CqlResult cqlresult;
        try
        {
            cqlresult = makeCqlResult(rows, 1);
        }
        catch (CharacterCodingException e)
        {
            throw new SQLTransientException(e);
        }
        
        result = new CassandraResultSet(statement,cqlresult);
        return result;
    }
    
    private class Entry
    {
        String name = null;
        ByteBuffer value = null;
        String type = null;
        
        private Entry(String name,ByteBuffer value,String type)
        {
            this.name = name;
            this.value = value;
            this.type = type;
        }        
    }

}
