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


public class TypedColumn
{
    public enum CollectionType {NOT_COLLECTION,MAP,LIST,SET};
    
    private final Column rawColumn;

    // we cache the frequently-accessed forms: java object for value, String for name.
    // Note that {N|V}.toString() isn't always the same as Type.getString
    // (a good example is byte buffers).
    private final Object value;
    private final String nameString;
    private final AbstractJdbcType<?> nameType, valueType, keyType;
    private final CollectionType collectionType;
    
    public TypedColumn(Column column, AbstractJdbcType<?> comparator, AbstractJdbcType<?> validator)
    {
        this(column,comparator, validator, null, CollectionType.NOT_COLLECTION);
    }
    
    public TypedColumn(Column column, AbstractJdbcType<?> nameType, AbstractJdbcType<?> valueType, AbstractJdbcType<?> keyType, CollectionType type)
    {
        rawColumn = column;
        this.collectionType = type;
        this.nameType = nameType;
        this.nameString = nameType.getString(column.name);
        this.valueType = valueType;
        this.keyType = keyType;
        
        if (column.value == null || !column.value.hasRemaining()) 
        {
            this.value = null;
        }
        else switch(collectionType)
        {
            case NOT_COLLECTION:
                this.value =  valueType.compose(column.value);
                break;
            case LIST:
                value = ListMaker.getInstance(valueType).compose(column.value);
                break;
            case SET:
                value = SetMaker.getInstance(valueType).compose(column.value);
                break;
            case MAP:
                value = MapMaker.getInstance(keyType, valueType).compose(column.value);
                break;
           default:
                value = null;
        }
    }


    public Column getRawColumn()
    {
        return rawColumn;
    }
    
    public Object getValue()
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
    
    public AbstractJdbcType getNameType()
    {
        return  nameType;
    }

    public AbstractJdbcType getValueType()
    {
        return valueType;
    }

    public CollectionType getCollectionType()
    {
        return collectionType;
    }
    

    public String toString()
    {
        return String.format("TypedColumn [rawColumn=%s, value=%s, nameString=%s, nameType=%s, valueType=%s, keyType=%s, collectionType=%s]",
            displayRawColumn(rawColumn),
            value,
            nameString,
            nameType,
            valueType,
            keyType,
            collectionType);
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
