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

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql.TestClass;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class HandleObjectsUnitTest
{

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {}

    @Test(expected=Exception.class)
    public void test0UnsupportedArray() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.ARRAY, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedBlob() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.BLOB, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedClob() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.CLOB, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedDistinct() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.DISTINCT, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedNclob() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.NCLOB, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedNull() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.NULL, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedOther() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.OTHER, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedRef() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.REF, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedSqlxml() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.SQLXML, 0);
    }

    @Test(expected=Exception.class)
    public void test0UnsupportedStruct() throws Exception
    {
        Object object = new String("This is a String");
    	HandleObjects.makeBytes(object, Types.STRUCT, 0);
    }
    
    @Test
    public void test1Varchar() throws Exception
    {
        Object object = new String("This is a String");
        ByteBuffer bb = HandleObjects.makeBytes(object, Types.VARCHAR, 0);
        String string = ByteBufferUtil.string(bb);
        assertEquals(object, string);
    }
    
    
    @Test
    public void test2Integer() throws Exception
    {
        Object object = new Integer(12345);
        ByteBuffer bb = HandleObjects.makeBytes(object, Types.INTEGER, 0);
        Integer integer = ByteBufferUtil.toInt(bb);
        assertEquals(object, integer);
        
        object = 12345;
        Class<?> objectclass = object.getClass();
        System.out.println("object class of 12345 is : " + objectclass );
        bb = HandleObjects.makeBytes(object, Types.INTEGER, 0);
        integer = ByteBufferUtil.toInt(bb);
        assertEquals(object, integer);
        
        object = new Long(123457890);
        bb = HandleObjects.makeBytes(object, Types.INTEGER, 0);
        integer = ByteBufferUtil.toInt(bb);
        Integer integeronly = ((Long)object).intValue();
        assertEquals(integeronly, integer);
        
        object = new BigDecimal(123457890.789);
        bb = HandleObjects.makeBytes(object, Types.INTEGER, 0);
        integer = ByteBufferUtil.toInt(bb);
        Integer integerpart = ((BigDecimal)object).intValue();
        
        assertEquals(integerpart, integer);
   }
    
    @Test
    public void test3Binary() throws Exception
    {
        String stringvalue = "A simple string value";
        Object object = stringvalue.getBytes();
        ByteBuffer bb = HandleObjects.makeBytes(object, Types.BINARY, 0);
        assertEquals(stringvalue, ByteBufferUtil.string(bb));             
    }

    @Test
    public void test4Boolean() throws Exception
    {
        Object object = 0;
        ByteBuffer bb = HandleObjects.makeBytes(object, Types.BOOLEAN, 0);
        assertFalse(JdbcBoolean.instance.compose(bb));
        
        object = 12345;
        bb = HandleObjects.makeBytes(object, Types.BOOLEAN, 0);
        assertTrue(JdbcBoolean.instance.compose(bb));
        
        object = 0.0;
        bb = HandleObjects.makeBytes(object, Types.BOOLEAN, 0);
        assertFalse(JdbcBoolean.instance.compose(bb));
        
        object = 12345.67;
        bb = HandleObjects.makeBytes(object, Types.BOOLEAN, 0);
        assertTrue(JdbcBoolean.instance.compose(bb));
        
        object = true;
        bb = HandleObjects.makeBytes(object, Types.BOOLEAN, 0);
        assertTrue(JdbcBoolean.instance.compose(bb));
    }

    @Test
    public void test5Date() throws Exception
    {
        
        Time time = new Time(3600L * 20);
        Object object = time;
        ByteBuffer bb = HandleObjects.makeBytes(object, Types.TIME, 0);
        assertEquals(time.getTime(), JdbcDate.instance.compose(bb).getTime());
        
        java.util.Date now = new java.util.Date();
        Date date = new Date(now.getTime());
        object = date;
        bb = HandleObjects.makeBytes(object, Types.DATE, 0);
        assertEquals(date.getTime(), JdbcDate.instance.compose(bb).getTime());             
    }

     @Test
    public void test99JavaObject() throws SQLException
    {
        List<String> myList = new ArrayList<String>();
        myList.add("A");
        myList.add("B");
        myList.add("C");
        Object object = new TestClass("This is a String", 3456,myList );
        ByteBuffer bb = HandleObjects.makeBytes(object, Types.JAVA_OBJECT, 0);
        System.out.println("Java object = "+ ByteBufferUtil.bytesToHex(bb));
    }
    
}
