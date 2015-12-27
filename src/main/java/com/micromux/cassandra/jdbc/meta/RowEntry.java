package com.micromux.cassandra.jdbc.meta;

import java.nio.ByteBuffer;

class RowEntry {

    static final String UTF8_TYPE = "UTF8Type";
    static final String ASCII_TYPE = "AsciiType";
    static final String INT32_TYPE = "Int32Type";
    static final String BOOLEAN_TYPE = "BooleanType";

    String name = null;
    ByteBuffer value = null;
    String type = null;
    Class clazz = null;

    RowEntry(String name, ByteBuffer value, Class clazz) {
        this.name = name;
        this.value = value;
        this.clazz = clazz;
    }

}
