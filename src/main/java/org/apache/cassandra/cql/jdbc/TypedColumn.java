package org.apache.cassandra.cql.jdbc;
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


import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.ByteBufferUtil;


public class TypedColumn<T>
{
    private final Column rawColumn;

    // we cache the frequently-accessed forms: java object for value, String for name.
    // Note that {N|V}.toString() isn't always the same as Type.getString
    // (a good example is byte buffers).
    private final T value;
    private final String nameString;
    private final AbstractJdbcType<T> nameType, valueType;

    public TypedColumn(Column column, AbstractJdbcType<T> comparator, AbstractJdbcType<T> validator)
    {
        rawColumn = column;
        this.value = (column.value == null || !column.value.hasRemaining()) ? null : validator.compose(column.value);
        nameString = comparator.getString(column.name);
        nameType = comparator;
        valueType = validator;
    }

    public Column getRawColumn()
    {
        return rawColumn;
    }
    
    public T getValue()
    {
        return value;
    }
    
    public String getNameString()
    {
        return nameString;
    }
    
    public String getValueString()
    {
        return valueType.getString(rawColumn.value);
    }
    
    public AbstractJdbcType<T> getNameType()
    {
        return nameType;
    }

    public AbstractJdbcType<T> getValueType()
    {
        return valueType;
    }

    public String toString()
    {
        return String.format("TypedColumn [rawColumn=%s, value=%s, nameString=%s, nameType=%s, valueType=%s]",
            displayRawColumn(rawColumn),
            value,
            nameString,
            nameType,
            valueType);
    }
    private String displayRawColumn(Column column)
    {
        String name;
        try
        {
            name = ByteBufferUtil.string(column.name);
        }
        catch (CharacterCodingException e)
        {
            name = "<binary>";
        }
        String value = (column.value==null)? "<null>" :ByteBufferUtil.bytesToHex(column.value);
        return String.format("Column[name=%s, value=%s]",name,value);
    }
}
