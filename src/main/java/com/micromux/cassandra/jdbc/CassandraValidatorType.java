package com.micromux.cassandra.jdbc;

import java.sql.Types;

public enum CassandraValidatorType {

    CompositeType("org.apache.cassandra.db.marshal.CompositeType", Types.STRUCT),
    SetType("org.apache.cassandra.db.marshal.SetType", Types.ARRAY),
    MapType("org.apache.cassandra.db.marshal.MapType", Types.JAVA_OBJECT),
    UTF8Type("org.apache.cassandra.db.marshal.UTF8Type", Types.LONGNVARCHAR),
    BooleanType("org.apache.cassandra.db.marshal.BooleanType", Types.BOOLEAN),
    Int32Type("org.apache.cassandra.db.marshal.Int32Type", Types.INTEGER),
    LongType("org.apache.cassandra.db.marshal.LongType", Types.BIGINT),
    DoubleType("org.apache.cassandra.db.marshal.DoubleType", Types.DOUBLE),
    DecimalType("org.apache.cassandra.db.marshal.DecimalType", Types.DECIMAL),
    BytesType("org.apache.cassandra.db.marshal.BytesType", Types.NCLOB),
    UUIDType("org.apache.cassandra.db.marshal.UUIDType", Types.JAVA_OBJECT),
    InetAddressType("org.apache.cassandra.db.marshal.InetAddressType", Types.JAVA_OBJECT),
    TimestampType("org.apache.cassandra.db.marshal.TimestampType", Types.TIMESTAMP),
    TimeUUIDType("org.org.apache.cassandra.db.marshal.TimeUUIDType", Types.JAVA_OBJECT),
    Unknown("Unknown", Types.OTHER);

    private String validatorName;
    int sqlType;

    CassandraValidatorType(String validatorName, int sqlType) {
        this.validatorName = validatorName;
        this.sqlType = sqlType;
    }

    /**
     * This is the JDBC database type identifier.
     * @return  JDBC type identifier.
     */
    public int getSqlType() {
        return sqlType;
    }

    public String getSqlName() {
        return validatorName.substring(validatorName.lastIndexOf('.') + 1);
    }

    /**
     * Convert a Cassandra validator into a validator type. Always default to the
     * {@code Unknown} type and then attempt to parse the name.
     * @param validator  Name of the validator to check
     * @return Resulting type.
     */
    public static CassandraValidatorType fromValidator(String validator) {

        CassandraValidatorType validatorType = Unknown;

        for (CassandraValidatorType cvt : CassandraValidatorType.values()) {
            if (validator.startsWith(cvt.validatorName)) {
                validatorType = cvt;
                break;
            }
        }

        return validatorType;

    }
}
