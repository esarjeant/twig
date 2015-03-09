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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.cassandra.cql.jdbc.JdbcDate.iso8601Patterns;

//import org.apache.commons.lang.time.DateUtils;

public class HandleObjects
{
    private static final Logger LOG = LoggerFactory.getLogger(HandleObjects.class);

    private static final String BAD_MAPPING = "encountered object of class: %s, but only '%s' is supported to map to %s";

    private static final String STR_BOOL_NUMERIC = "String, Boolean, or a Numeric class";
    
    private final static Map<Class<?>, AbstractJdbcType<?>> map = new HashMap<Class<?>, AbstractJdbcType<?>>();

    static
    {
        map.put(Boolean.class, JdbcBoolean.instance);
        map.put(Byte[].class, JdbcBytes.instance);
        map.put(java.util.Date.class, JdbcDate.instance);
        map.put(BigDecimal.class, JdbcDecimal.instance);
        map.put(Double.class, JdbcDouble.instance);
        map.put(Float.class, JdbcFloat.instance);
        map.put(Integer.class, JdbcInt32.instance);
        map.put(Inet4Address.class, JdbcInetAddress.instance);
        map.put(Inet6Address.class, JdbcInetAddress.instance);
        map.put(BigInteger.class, JdbcInteger.instance);
        map.put(Long.class, JdbcLong.instance);
        map.put(String.class, JdbcUTF8.instance);
        map.put(UUID.class, JdbcUUID.instance);
    }

    private static AbstractJdbcType<?> getType(Class<?> elementClass) throws SQLException
    {
        AbstractJdbcType<?> type = map.get(elementClass);
    //    if (type==null) throw new SQLRecoverableException(String.format("unsupported Collection element type:  '%s' for CQL", elementClass));
        return type;
    }


    private static Long fromString(String source) throws SQLException
    {
      long millis = 0;
      if (source.isEmpty() ||source.toLowerCase().equals("now")) return System.currentTimeMillis();
      // Milliseconds since epoch?
      else if (source.matches("^\\d+$"))
      {
          try
          {
              Long.parseLong(source);
          }
          catch (NumberFormatException e)
          {
              throw new SQLNonTransientException(String.format("unable to make long (for date) from:  '%s'", source), e);
          }
      }
      // Last chance, attempt to parse as date-time string
      else
      {
          try
          {
              millis = DateUtils.parseDate(source, iso8601Patterns).getTime();
          }
          catch (ParseException e1)
          {
              throw new SQLNonTransientException(String.format("unable to coerce '%s' to a  formatted date (long)", source), e1);
          }
      }
      return millis;
    }

    private static final SQLException makeBadMapping(Class<?> badclass, String javatype, String jdbctype)
    {
        return new SQLNonTransientException(String.format(BAD_MAPPING, badclass, javatype, jdbctype));
    }

    private static final ByteBuffer javaObject(Object object) throws SQLException
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

        return ByteBuffer.wrap(bytes);
    }

    private static final Integer objectToINTEGER(Class<? extends Object> objectClass, Object object)
    {
        // Strings should always work
        if (objectClass == String.class) return Integer.valueOf((String) object);

        // Booleans are either false=0 or true=1
        if (objectClass == Boolean.class) return ((Boolean) object) == false ? 0 : 1;

        // All the integer (non floating-point) are simple
        if (objectClass == Integer.class) return (Integer) object;
        else if (objectClass == BigInteger.class) return ((BigInteger) object).intValue();
        else if (objectClass == Long.class) return ((Long) object).intValue();
        else if (objectClass == Short.class) return ((Short) object).intValue();
        else if (objectClass == Byte.class) return ((Byte) object).intValue();

        // Floating ones need to just pass the integer part
        else if (objectClass == Double.class) return Integer.valueOf(((Double) object).intValue());
        else if (objectClass == Float.class) return Integer.valueOf(((Float) object).intValue());
        else if (objectClass == BigDecimal.class) return Integer.valueOf(((BigDecimal) object).intValue());
        else return null; // this should not happen
    }

    private static final Long objectToBIGINT(Class<? extends Object> objectClass, Object object)
    {
        // Strings should always work
        if (objectClass == String.class) return Long.valueOf((String) object);

        // Booleans are either false=0 or true=1
        if (objectClass == Boolean.class) return ((Boolean) object) == false ? 0L : 1L;

        // All the integer (non floating-point) are simple
        if (objectClass == Integer.class) return Long.valueOf((Integer) object);
        else if (objectClass == BigInteger.class) return ((BigInteger) object).longValue();
        else if (objectClass == Long.class) return ((Long) object).longValue();
        else if (objectClass == Short.class) return ((Short) object).longValue();
        else if (objectClass == Byte.class) return ((Byte) object).longValue();

        // Floating ones need to just pass the integer part
        else if (objectClass == Double.class) return Long.valueOf(((Double) object).longValue());
        else if (objectClass == Float.class) return Long.valueOf(((Float) object).longValue());
        else if (objectClass == BigDecimal.class) return Long.valueOf(((BigDecimal) object).longValue());
        else return null; // this should not happen
    }

    private static final Long objectToDATEorTIMEorTIMESTAMP(Class<? extends Object> objectClass, Object object)  throws SQLException
    {
        // Strings should always work
        if (objectClass == String.class) return fromString((String) object);
        
        if (objectClass == java.util.Date.class) return ((java.util.Date) object).getTime();
        else if (objectClass == Date.class) return ((Date) object).getTime();
        else if (objectClass == Time.class) return ((Time) object).getTime();
        else if (objectClass == Timestamp.class) return ((Timestamp) object).getTime();
        else return null; // this should not happen
    }

    private static final Boolean objectToBOOLEAN(Class<? extends Object> objectClass, Object object)
    {
        // Strings should always work
        if (objectClass == String.class) return Boolean.valueOf((String) object);

        // Booleans are either false=0 or true=1
        if (objectClass == Boolean.class) return ((Boolean) object);

        // All the integer (non floating-point) are simple
        if (objectClass == Integer.class) return ((Integer) object) == 0 ? false : true;
        else if (objectClass == BigInteger.class) return ((BigInteger) object).intValue() == 0 ? false : true;
        else if (objectClass == Long.class) return ((Long) object) == 0 ? false : true;
        else if (objectClass == Short.class) return ((Short) object) == 0 ? false : true;
        else if (objectClass == Byte.class) return ((Byte) object) == 0 ? false : true;

        // Floating ones need to just pass the integer part
        else if (objectClass == Double.class) return Integer.valueOf(((Double) object).intValue()) == 0 ? false : true;
        else if (objectClass == Float.class) return Integer.valueOf(((Float) object).intValue()) == 0 ? false : true;
        else if (objectClass == BigDecimal.class) return Integer.valueOf(((BigDecimal) object).intValue()) == 0 ? false : true;
        else return null; // this should not happen
    }

    private static final BigInteger objectToBITorTINYINTorSMALLINTorNUMERIC(Class<? extends Object> objectClass, Object object)
    {
        // Strings should always work
        if (objectClass == String.class) return new BigInteger((String) object);

        // Booleans are either false=0 or true=1
        if (objectClass == Boolean.class) return ((Boolean) object) == false ? BigInteger.ZERO : BigInteger.ONE;

        // All the integer (non floating-point) are simple
        if (objectClass == Integer.class) return BigInteger.valueOf((Integer) object);
        else if (objectClass == BigInteger.class) return ((BigInteger) object);
        else if (objectClass == Long.class) return BigInteger.valueOf(((Long) object));
        else if (objectClass == Short.class) return BigInteger.valueOf(((Short) object).longValue());
        else if (objectClass == Byte.class) return BigInteger.valueOf(((Byte) object).longValue());

        // Floating ones need to just pass the integer part
        else if (objectClass == Double.class) return BigInteger.valueOf(((Double) object).longValue());
        else if (objectClass == Float.class) return BigInteger.valueOf(((Float) object).longValue());
        else if (objectClass == BigDecimal.class) return BigInteger.valueOf(((BigDecimal) object).intValue());
        else return null; // this should not happen
    }

    
    private static final Class<?> getCollectionElementType(Object maybeCollection)
    {
        Collection trial = (Collection) maybeCollection;
        if (trial.isEmpty()) return trial.getClass();
        else return trial.iterator().next().getClass();
    }
   
    private static final Class<?> getKeyElementType(Object maybeMap)
    {
        return getCollectionElementType(((Map) maybeMap).keySet());
    }
   
    private static final Class<?> getValueElementType(Object maybeMap)
    {
        return getCollectionElementType(((Map) maybeMap).values());
    }
   
    private  static final <X> ByteBuffer makeByteBuffer4List(AbstractJdbcType<?> instanceType, List<X> value)
    {
        return ListMaker.getInstance(instanceType).decompose(value.getClass().cast(value));
    }
    
    private  static final <X> ByteBuffer makeByteBuffer4Set(AbstractJdbcType<?> instanceType, Set<X> value)
    {
        return SetMaker.getInstance(instanceType).decompose(value.getClass().cast(value));
    }
    
    private  static final <K,V> ByteBuffer makeByteBuffer4Map(AbstractJdbcType<?> keyInstanceType, AbstractJdbcType<?> valueInstanceType, Map<K,V> value)
    {
        return MapMaker.getInstance(keyInstanceType,valueInstanceType).decompose(value.getClass().cast(value));
    }
    
    private static final <X> ByteBuffer handleAsList(Class<? extends Object> objectClass, Object object) throws SQLException
    {
        if (!List.class.isAssignableFrom(objectClass)) return ByteBuffer.wrap(new byte[0]);

        Class<?> elementClass = getCollectionElementType(object);
        
        AbstractJdbcType<?> instanceType = (elementClass==null)? JdbcUTF8.instance : getType(elementClass);
        
        ByteBuffer bb =  makeByteBuffer4List(instanceType, (List<X>) object.getClass().cast(object));
        
        return bb;
    }

    private static final <X> ByteBuffer handleAsSet(Class<? extends Object> objectClass, Object object) throws SQLException
    {
        if (!Set.class.isAssignableFrom(objectClass)) return ByteBuffer.wrap(new byte[0]);

        Class<?> elementClass = getCollectionElementType(object);
        
        AbstractJdbcType<?> instanceType = (elementClass==null)? JdbcUTF8.instance : getType(elementClass);
        
        ByteBuffer bb =  makeByteBuffer4Set(instanceType, (Set<X>) object.getClass().cast(object));
        
        return bb;
    }

    private static final <K,V> ByteBuffer handleAsMap(Class<? extends Object> objectClass, Object object) throws SQLException
    {
        if (!Map.class.isAssignableFrom(objectClass)) return ByteBuffer.wrap(new byte[0]);

        Class<?> keyElementClass = getKeyElementType(object);
        Class<?> valueElementClass = getValueElementType(object);
        
        AbstractJdbcType<?> keyInstanceType = getType(keyElementClass);
        AbstractJdbcType<?> valueInstanceType = getType(valueElementClass);
        
        ByteBuffer bb =  makeByteBuffer4Map(keyInstanceType,valueInstanceType, (Map<K,V>) object.getClass().cast(object));
        
        return bb;
    }

    public static final ByteBuffer makeBytes(Object object, int baseType, int scaleOrLength) throws SQLException
    {
        Class<? extends Object> objectClass = object.getClass();
        boolean isCollection = (Collection.class.isAssignableFrom(objectClass));
        int targetSqlType = isCollection ? Types.OTHER : baseType;
        // Type check first
        switch (targetSqlType)
        {
            case Types.TINYINT:
                // Only Numeric classes, Strings and Booleans are supported for transformation to TINYINT
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"TINYINT");
                break;

            case Types.SMALLINT:
                // Only Numeric classes, Strings and Booleans are supported for transformation to SMALLINT
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"SMALLINT");
                break;


            case Types.INTEGER:
                // Only Numeric classes, Strings and Booleans are supported for transformation to INTEGER
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"INTEGER");
                break;

            case Types.BIGINT:
                // Only Numeric classes, Strings and Booleans are supported for transformation to BIGINT
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"BIGINT");
                break;

            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
                // Only Numeric classes Strings and Booleans are supported for transformation to REAL,FLOAT,DOUBLE,DECIMAL
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"the floating point types");
                break;

            case Types.NUMERIC:
                //NB This as a special case variation for Cassandra!! NUMERIC is transformed to java BigInteger (varint CQL type)
                //
                // Only Numeric classes Strings and Booleans are supported for transformation to NUMERIC
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"NUMERIC");
                break;
                
            case Types.BIT:
                // Only Numeric classes Strings and Booleans are supported for transformation to BIT
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"BIT");
                break;

            case Types.BOOLEAN:
                // Only Numeric classes Strings and Booleans are supported for transformation to BOOLEAN
                if (!(objectClass == String.class || objectClass == Boolean.class || Number.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"BOOLEAN");
                break;

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if (!objectClass.isAssignableFrom(String.class))
                    throw makeBadMapping(objectClass, "String", "the various VARCHAR types");
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (!(objectClass.isAssignableFrom(ByteBuffer.class) || objectClass.getSimpleName().equals("byte[]"))) 
                    throw makeBadMapping(objectClass,"ByteBuffer or byte[]","the BINARY Types");
                break;

            case Types.DATE:
                if (!(objectClass == String.class || objectClass == java.util.Date.class || objectClass == Date.class || objectClass == Timestamp.class)) 
                    throw makeBadMapping(objectClass,"String, Date(java and sql) or Timestamp types","DATE");
                break;

            case Types.TIME:
                if (!(objectClass == String.class || objectClass == java.util.Date.class || objectClass == Time.class || objectClass == Timestamp.class)) 
                    throw makeBadMapping(objectClass,"String, Date (java), Time or Timestamp types","TIME");
                break;

            case Types.TIMESTAMP:
                if (!(objectClass == String.class || objectClass == java.util.Date.class || objectClass == Date.class || objectClass == Timestamp.class)) 
                    throw makeBadMapping(objectClass,"String, Date(java and sql) or Timestamp types","TIMESTAMP");
                break;

            case Types.DATALINK:
                if (objectClass != URL.class) throw makeBadMapping(objectClass,"a URL type","DATALINK");
                break;

            case Types.JAVA_OBJECT:
                break;

            case Types.OTHER:
                // Only Collection classes for transformation to OTHER
                if (!( List.class.isAssignableFrom(object.getClass()) 
                    || Set.class.isAssignableFrom(object.getClass())
                    || Map.class.isAssignableFrom(object.getClass())))
                    throw makeBadMapping(objectClass,STR_BOOL_NUMERIC,"OTHER");
                break;

            case Types.ROWID:
                if (objectClass != RowId.class) throw makeBadMapping(objectClass,"a RowId type","ROWID");
                break;

            default:
                throw new SQLNonTransientException("Unsupported transformation to Jdbc Type: "+ targetSqlType);
        }
        
        // see if we can map to an supported Type
        switch (targetSqlType)
        {
            case Types.BIT:
                BigInteger bitvalue = objectToBITorTINYINTorSMALLINTorNUMERIC(objectClass, object);
                assert bitvalue != null;
                return JdbcInteger.instance.decompose((bitvalue == BigInteger.ZERO) ? BigInteger.ZERO : BigInteger.ONE);
                
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.NUMERIC:
                BigInteger varint = objectToBITorTINYINTorSMALLINTorNUMERIC(objectClass, object);
                assert varint != null;
                return JdbcInteger.instance.decompose(varint);

            case Types.INTEGER:
                Integer value = objectToINTEGER(objectClass, object);
                assert value != null;
                return JdbcInt32.instance.decompose(value);

            case Types.BIGINT:
                Long longvalue = objectToBIGINT(objectClass, object);
                assert longvalue != null;
                return JdbcLong.instance.decompose(longvalue);

            case Types.BOOLEAN:
                Boolean bool = objectToBOOLEAN(objectClass, object);
                assert bool != null;
                return JdbcBoolean.instance.decompose(bool);

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                    return ByteBufferUtil.bytes((String) object);

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (objectClass.isAssignableFrom(ByteBuffer.class))
                {
                    return ((ByteBuffer) object);
                }
                else if (objectClass.getSimpleName().equals("byte[]"))
                {
                    return ByteBuffer.wrap((byte[]) object);
                }
                else return null; // this should not happen


            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                Long millis = objectToDATEorTIMEorTIMESTAMP(objectClass, object);
                assert millis != null;
                return JdbcLong.instance.decompose(millis); 
                
            case Types.DATALINK:
                String urlAsString = ((URL) object).toExternalForm();
                return JdbcUTF8.instance.decompose(urlAsString);                 

            case Types.JAVA_OBJECT:
                return javaObject(object);
                
            case Types.OTHER:
                if ( List.class.isAssignableFrom(objectClass))
                {
                  return handleAsList(objectClass, object);  
                }
                else if ( Set.class.isAssignableFrom(objectClass))
                {
                    return handleAsSet(objectClass, object);  
                }
                else if ( Map.class.isAssignableFrom(objectClass))
                {
                    return handleAsMap(objectClass, object);  
                }
                else return null;
                

            case Types.ROWID:
                byte[] bytes = ((RowId) object).getBytes();
                return ByteBuffer.wrap(bytes);                 

            default:
                LOG.warn("Unhandled JDBC type: " + targetSqlType);
                return null;
        }
    }
}
