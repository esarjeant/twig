package org.apache.cassandra.cql.jdbc.meta;

public class CassandraColumn<T> {
    private T value;
    private String name;

    private long timestamp;

    public CassandraColumn(String name, T value) {
        this.name = name;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }

    public T getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getName() {
        return name;
    }

    public final int getLength() {

        if (value instanceof String) {
            return Integer.MAX_VALUE;
        } else if ((value instanceof Integer) || (value instanceof Boolean)) {
            return 4;
        } else if (value instanceof Long) {
            return 8;
        } else {
            return 20; // arbitrary amount for anything else...
        }

    }
}
