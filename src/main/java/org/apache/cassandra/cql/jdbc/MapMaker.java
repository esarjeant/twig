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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapMaker<K, V>
{
    // interning instances
    private static final Map<Pair<AbstractJdbcType<?>, AbstractJdbcType<?>>, MapMaker> instances = new HashMap<Pair<AbstractJdbcType<?>, AbstractJdbcType<?>>, MapMaker>();

    public final AbstractJdbcType<K> keys;
    public final AbstractJdbcType<V> values;

    public static synchronized <K, V> MapMaker<K, V> getInstance(AbstractJdbcType<K> keys, AbstractJdbcType<V> values)
    {
        Pair<AbstractJdbcType<?>, AbstractJdbcType<?>> p = Pair.<AbstractJdbcType<?>, AbstractJdbcType<?>>create(keys, values);
        MapMaker<K, V> t = instances.get(p);
        if (t == null)
        {
            t = new MapMaker<K, V>(keys, values);
            instances.put(p, t);
        }
        return t;
    }

    private MapMaker(AbstractJdbcType<K> keys, AbstractJdbcType<V> values)
    {
        this.keys = keys;
        this.values = values;
    }

    public Map<K, V> compose(ByteBuffer bytes)
    {
        ByteBuffer input = bytes.duplicate();
        int n = input.getShort();
        Map<K, V> m = new LinkedHashMap<K, V>(n);
        for (int i = 0; i < n; i++)
        {
            int sk = input.getShort();
            byte[] datak = new byte[sk];
            input.get(datak);
            ByteBuffer kbb = ByteBuffer.wrap(datak);

            int sv = input.getShort();
            byte[] datav = new byte[sv];
            input.get(datav);
            ByteBuffer vbb = ByteBuffer.wrap(datav);

            m.put(keys.compose(kbb), values.compose(vbb));
        }
        return m;
    }

    /**
     * Layout is: {@code <n><sk_1><k_1><sv_1><v_1>...<sk_n><k_n><sv_n><v_n> }
     * where:
     *   n is the number of elements
     *   sk_i is the number of bytes composing the ith key k_i
     *   k_i is the sk_i bytes composing the ith key
     *   sv_i is the number of bytes composing the ith value v_i
     *   v_i is the sv_i bytes composing the ith value
     */
    public ByteBuffer decompose(Map<K, V> value)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
        int size = 0;
        for (Map.Entry<K, V> entry : value.entrySet())
        {
            ByteBuffer bbk = keys.decompose(entry.getKey());
            ByteBuffer bbv = values.decompose(entry.getValue());
            bbs.add(bbk);
            bbs.add(bbv);
            size += 4 + bbk.remaining() + bbv.remaining();
        }
        return Utils.pack(bbs, value.size(), size);
    }

}
