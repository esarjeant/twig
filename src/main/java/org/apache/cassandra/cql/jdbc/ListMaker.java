 /*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql.jdbc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListMaker<T>
{
    // interning instances
    private static final Map<AbstractJdbcType<?>, ListMaker> instances = new HashMap<AbstractJdbcType<?>, ListMaker>();

    public final AbstractJdbcType<T> elements;


    public static synchronized <T> ListMaker<T> getInstance(AbstractJdbcType<T> elements)
    {
        ListMaker<T> t = instances.get(elements);
        if (t == null)
        {
            t = new ListMaker<T>(elements);
            instances.put(elements, t);
        }
        return t;
    }
    
    private ListMaker(AbstractJdbcType<T> elements)
    {
        this.elements = elements;
    }

    public List<T> compose(ByteBuffer bytes)
    {
        ByteBuffer input = bytes.duplicate();
        int n = input.getShort();
        List<T> l = new ArrayList<T>(n);
        for (int i = 0; i < n; i++)
        {
            int s = input.getShort();
            byte[] data = new byte[s];
            input.get(data);
            ByteBuffer databb = ByteBuffer.wrap(data);
            l.add(elements.compose(databb));
        }
        return l;
    }
    
    /**
     * Layout is: {@code <n><s_1><b_1>...<s_n><b_n> }
     * where:
     *   n is the number of elements
     *   s_i is the number of bytes composing the ith element
     *   b_i is the s_i bytes composing the ith element
     */
    public ByteBuffer decompose(List<T> value)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
        int size = 0;
        for (T elt : value)
        {
            ByteBuffer bb = elements.decompose(elt);
            bbs.add(bb);
            size += 2 + bb.remaining();
        }
        return Utils.pack(bbs, value.size(), size);
    }
}
