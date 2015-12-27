package com.micromux.cassandra.jdbc;

import java.sql.Types;

public enum CassandraValidatorType {

    CompositeType("org.apache.cassandra.db.marshal.CompositeType", Types.STRUCT, 20, 0),
    SetType("org.apache.cassandra.db.marshal.SetType", Types.ARRAY, 40, 0),
    MapType("org.apache.cassandra.db.marshal.MapType", Types.JAVA_OBJECT, 40, 0),
    UTF8Type("org.apache.cassandra.db.marshal.UTF8Type", Types.VARCHAR, 25, 0),
    BooleanType("org.apache.cassandra.db.marshal.BooleanType", Types.BOOLEAN, 1, 0),
    Int32Type("org.apache.cassandra.db.marshal.Int32Type", Types.INTEGER, 8, 2),
    LongType("org.apache.cassandra.db.marshal.LongType", Types.BIGINT, 10, 2),
    DoubleType("org.apache.cassandra.db.marshal.DoubleType", Types.DOUBLE, 10, 10),
    DecimalType("org.apache.cassandra.db.marshal.DecimalType", Types.DECIMAL, 10, 10),
    BytesType("org.apache.cassandra.db.marshal.BytesType", Types.NCLOB, 50, 0),
    UUIDType("org.apache.cassandra.db.marshal.UUIDType", Types.JAVA_OBJECT, 50, 0),
    InetAddressType("org.apache.cassandra.db.marshal.InetAddressType", Types.JAVA_OBJECT, 10, 0),
    TimestampType("org.apache.cassandra.db.marshal.TimestampType", Types.TIMESTAMP, 25, 0),
    TimeUUIDType("org.apache.cassandra.db.marshal.TimeUUIDType", Types.JAVA_OBJECT, 50, 0),
    Unknown("Unknown", Types.OTHER, 20, 0);

    private String validatorName;
    int sqlType;
    int sqlWidth;
    int sqlRadix;

    CassandraValidatorType(String validatorName, int sqlType, int sqlWidth, int sqlRadix) {
        this.validatorName = validatorName;
        this.sqlType = sqlType;
        this.sqlWidth = sqlWidth;
        this.sqlRadix = sqlRadix;
    }

    /**
     * This is the JDBC database type identifier.
     * @return  JDBC type identifier.
     */
    public int getSqlType() {
        return sqlType;
    }

    public int getSqlWidth() {
        return sqlWidth;
    }

    public int getSqlRadix() {
        return sqlRadix;
    }

    public String getSqlName() {
        return validatorName.substring(validatorName.lastIndexOf('.') + 1);
    }

    public String getSqlDisplayName() {
        return getSqlName().toLowerCase().replace("type", "");
    }

    /**
     * This is the maximum length in bytes for the designated type.
     * @return Maximum length in bytes.
     */
    public int getSqlLength() {

        switch (this) {

            case UTF8Type:
            case BytesType:
                return Integer.MAX_VALUE / 2;

            case BooleanType:
                return 1;

            case Int32Type:
                return Integer.SIZE;

            case LongType:
                return Long.SIZE;

            case DoubleType:
            case DecimalType:
                return Double.SIZE;

            case UUIDType:
            case TimeUUIDType:
                return 36;

            case InetAddressType:
                return 24;

            case TimestampType:
                return Long.SIZE;

            default:
                return Integer.MAX_VALUE;

        }

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
            if (validator.contains(cvt.validatorName)) {
                validatorType = cvt;
                break;
            }
        }

        return validatorType;

    }
}
