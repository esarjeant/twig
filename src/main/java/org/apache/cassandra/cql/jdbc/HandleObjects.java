/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.cql.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Types;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author rickshaw
 * 
 */
public class HandleObjects
{
    private static final Logger LOG = LoggerFactory.getLogger(HandleObjects.class);

    private static final String BAD_MAPPING = "encountered object of class: %s, but only '%s' is supported to map to %s";


    private static final SQLException makeBadMapping(Class<?> badclass, String javatype, String jdbctype)
    {
        return new SQLNonTransientException(String.format(BAD_MAPPING, badclass, javatype, jdbctype));
    }

    private static final String bufferAsHex(ByteBuffer buffer)
    {
        return ByteBufferUtil.bytesToHex(buffer);
    }

    private static final String javaObject(Object object) throws SQLException
    {
        byte[] bytes = null;
        try
        {
            // Serialize to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.close();

            // Get the bytes of the serialized object
            bytes = bos.toByteArray();
        }
        catch (IOException e)
        {
            throw new SQLNonTransientException("Problem serializing the Java object", e);
        }

        return bufferAsHex(ByteBuffer.wrap(bytes));
    }


    public static final String makeString(Object object, int targetSqlType, int scaleOrLength) throws SQLException
    {
        Class<? extends Object> objectClass = object.getClass();

        // see if we can map to an supported AbstractType
        switch (targetSqlType)
        {
            case Types.JAVA_OBJECT:
                return javaObject(object);

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (objectClass.isAssignableFrom(ByteBuffer.class))
                {
                    return bufferAsHex(((ByteBuffer) object));
                }
                else throw makeBadMapping(objectClass, "ByteBuffer", "BINARY");

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if (objectClass.isAssignableFrom(String.class))
                {
                    return object.toString();
                }
                else throw makeBadMapping(objectClass, "String", "the various VARCHAR types");

            case Types.INTEGER:
                // Strings should always work 
                if (objectClass == String.class) return object.toString();
                
                // Only Numeric classes (besides String)
                if (!Number.class.isAssignableFrom(object.getClass())) throw makeBadMapping(objectClass, "a Numeric class (or String)", "INTEGER");
                
                // All the integer (non floating-point) are simple
                if (objectClass == Integer.class || objectClass == BigInteger.class || objectClass == Long.class ||
                    objectClass == Short.class || objectClass == Byte.class) return object.toString();
                
                // Floating ones need to just pass the integer part
                else if (objectClass == Double.class) return Integer.valueOf(((Double) object).intValue()).toString();
                else if (objectClass == Float.class) return Integer.valueOf(((Float) object).intValue()).toString();
                else if (objectClass == BigDecimal.class) return Integer.valueOf(((BigDecimal) object).intValue()).toString();

            default:
                LOG.warn("Unhandled JDBC type: "+targetSqlType);
                return null;
        }
    }
}
