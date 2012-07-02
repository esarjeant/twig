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

import java.nio.ByteBuffer;
import java.sql.SQLException;
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



public abstract class MetadataResultSets
{
    
    private static final String UTF8_TYPE = "UTF8Type";
    private static final String ASCII_TYPE = "AsciiType";
    
    private static final String[][][] TABLE_TYPES = { { {"TABLE_TYPE","SYSTEM TABLE"}   },
                                                    {   {"TABLE_TYPE","TABLE"}          } };
    
    /**
     * Make a {@code Column} from a key name and a value.
     * 
     * @param name
     *          the name of the key
     * @param value
     *          the value of the column as a {@code byte[]}
     * 
     * @return {@code Column}
     */
    private static final Column makeColumn(String name, String value)
    {
      return new Column(bytes(name)).setValue(bytes(value)).setTimestamp(System.currentTimeMillis());
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

    private static CqlResult makeCqlResult(String[][][] rowsOfcolsOfKvps, int position)
    {
        CqlResult result = new CqlResult(CqlResultType.ROWS);
        CqlMetadata meta = null;
        CqlRow row = null;
        Column column = null;
        List<Column> columnlist = new LinkedList<Column>();
        List<CqlRow> rowlist = new LinkedList<CqlRow>();
        List<String> colNamesList = new ArrayList<String>();
        
        int rowsize =  rowsOfcolsOfKvps.length;
        int colsize =  rowsOfcolsOfKvps[0].length;
        int entrysize =  rowsOfcolsOfKvps[0][0].length;
        
        for (int rowcnt = 0; rowcnt < rowsOfcolsOfKvps.length; rowcnt++ )
        {
            colNamesList = new ArrayList<String>();
            columnlist = new LinkedList<Column>();
            for (int colcnt = 0; colcnt < rowsOfcolsOfKvps[0].length; colcnt++ )
            {
                column = makeColumn(rowsOfcolsOfKvps[rowcnt][colcnt][0],rowsOfcolsOfKvps[rowcnt][colcnt][1]);
                columnlist.add(column);
                colNamesList.add(rowsOfcolsOfKvps[rowcnt][colcnt][0]);
            }
            row = makeRow(rowsOfcolsOfKvps[rowcnt][position-1][0],columnlist);
            rowlist.add(row);
        }
        
        meta = makeMetadataAllString(colNamesList);
        result.setSchema(meta).setRows(rowlist);
        return result;
    }

    public static CassandraResultSet makeTableTypes(CassandraStatement statement) throws SQLException
    {
        // use the global TABLE_TYPES with the key in column number 1 (one based)
        CqlResult cqlresult =  makeCqlResult(TABLE_TYPES, 1);
        
        CassandraResultSet result = new CassandraResultSet(statement,cqlresult);
        return result;
    }

}
