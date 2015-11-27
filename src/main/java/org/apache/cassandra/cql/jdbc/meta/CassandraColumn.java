package org.apache.cassandra.cql.jdbc.meta;

import java.nio.ByteBuffer;

public class CassandraColumn {
    private ByteBuffer value;
    private long timestamp;
    private String name;

    public CassandraColumn(String name) {
        this.name = name;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getName() {
        return name;
    }
}
