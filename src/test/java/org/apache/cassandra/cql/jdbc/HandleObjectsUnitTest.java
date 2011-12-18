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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql.TestClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author rickshaw
 *
 */
public class HandleObjectsUnitTest
{

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {}

    @Test
    public void test1Varchar() throws SQLException
    {
        Object object = new String("This is a String");
        String string = HandleObjects.makeString(object, Types.VARCHAR, 0);
        assertEquals("This is a String", string);
    }
    
    
    @Test
    public void test2Integer() throws SQLException
    {
        Object object = new Integer(12345);
        String string = HandleObjects.makeString(object, Types.INTEGER, 0);
        assertEquals("12345", string);
        
        object = new Long(123457890);
        string = HandleObjects.makeString(object, Types.INTEGER, 0);
        assertEquals("123457890", string);
        
        object = new BigDecimal(123457890.789);
        string = HandleObjects.makeString(object, Types.INTEGER, 0);
        assertEquals("123457890", string);
    }

    @Test
    public void test9JavaObject() throws SQLException
    {
        List<String> myList = new ArrayList<String>();
        myList.add("A");
        myList.add("B");
        myList.add("C");
        Object object = new TestClass("This is a String", 3456,myList );
        String string = HandleObjects.makeString(object, Types.JAVA_OBJECT, 0);
        System.out.println("Java object = "+ string);
    }
    
}
