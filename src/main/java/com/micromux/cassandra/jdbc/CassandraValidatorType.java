package com.micromux.cassandra.jdbc;

import com.datastax.driver.core.DataType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Types;

public enum CassandraValidatorType {

    //    counter
//    decimal
//    cdouble
//    cfloat
//    inet
//    tinyint
//    smallint
//    cint
//    timestamp
//    date
//    time
//    uuid
//    varchar
//    varint
//    timeuuid

//    ascii
//    text
//                    bigint

    CompositeType(DataType.Name.CUSTOM, Types.STRUCT, 20, 0),
    SetType(DataType.Name.SET, Types.ARRAY, 40, 0),
    MapType(DataType.Name.MAP, Types.JAVA_OBJECT, 40, 0),
    UdtType(DataType.Name.UDT, Types.JAVA_OBJECT, 40, 0),
    UTF8Type(DataType.Name.TEXT, Types.VARCHAR, 25, 0),
    BooleanType(DataType.Name.BOOLEAN, Types.BOOLEAN, 1, 0),
    Int32Type(DataType.Name.INT, Types.INTEGER, 8, 2),
    SmallIntType(DataType.Name.SMALLINT, Types.INTEGER, 8, 2),
    TinyIntType(DataType.Name.TINYINT, Types.INTEGER, 8, 2),
    LongType(DataType.Name.BIGINT, Types.BIGINT, 10, 2),
    DoubleType(DataType.Name.DOUBLE, Types.DOUBLE, 10, 10),
    DecimalType(DataType.Name.DECIMAL, Types.DECIMAL, 10, 10),
    BytesType(DataType.Name.BLOB, Types.NCLOB, 50, 0),
    UUIDType(DataType.Name.UUID, Types.JAVA_OBJECT, 50, 0),
    InetAddressType(DataType.Name.INET, Types.JAVA_OBJECT, 10, 0),
    TimestampType(DataType.Name.TIMESTAMP, Types.TIMESTAMP, 25, 0),
    TimeUUIDType(DataType.Name.TIMEUUID, Types.JAVA_OBJECT, 50, 0),
    Unknown(DataType.Name.CUSTOM, Types.JAVA_OBJECT, 50, 0);

    private DataType.Name validatorName;
    int sqlType;
    int sqlWidth;
    int sqlRadix;

    CassandraValidatorType(DataType.Name validatorName, int sqlType, int sqlWidth, int sqlRadix) {
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
        return validatorName.name();
    }

    public DataType.Name getName() {
        return validatorName;
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
     * @param dataType  Datatype to lookup
     * @return Resulting validator for type.
     */
    public static CassandraValidatorType fromValidator(DataType dataType) {


        CassandraValidatorType validatorType = Unknown;

        try {

            for (CassandraValidatorType cvt : CassandraValidatorType.values()) {
                if (StringUtils.equalsIgnoreCase(dataType.getName().name(), cvt.getName().name())) {
                    validatorType = cvt;
                    break;
                }
            }

        } catch (IllegalArgumentException ix) {
            // requested validator does not exist...
        }

        return validatorType;

    }

}
