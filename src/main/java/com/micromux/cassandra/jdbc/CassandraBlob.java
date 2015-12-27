package com.micromux.cassandra.jdbc;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

public class CassandraBlob implements Blob {

    private ByteBuffer buffer = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    private int length = 0;

    public CassandraBlob(ByteBuffer buffer) {
        this.buffer = buffer;
        this.length = buffer.array().length;
    }

    public CassandraBlob(String buffer) {
        this.buffer = ByteBufferUtil.bytes(buffer);
        this.length = buffer.length();
    }

    public CassandraBlob(InputStream inputStream) throws SQLException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int c;

        this.length = 0;

        try {

            while ((c = inputStream.read()) != -1){
                out.write((byte) c);
                this.length++;
            }

            // convert to byte buffer
            this.buffer = ByteBuffer.allocate(out.size());
            this.buffer.put(out.toByteArray());

            this.buffer.flip();

        } catch (IOException ix) {
            throw new SQLException("InputStream Fails to Read", ix);
        }

    }

    @Override
    public long length() throws SQLException {
        return this.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        // TODO: partial reads for blobs?
        return ByteBufferUtil.getArray(buffer);
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return ByteBufferUtil.inputStream(this.buffer);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        return 0;
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return 0;
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        // TODO: support this operation?
        return null;
    }

    @Override
    public void truncate(long len) throws SQLException {
        buffer = ByteBuffer.allocate((int)len);
    }

    @Override
    public void free() throws SQLException {
        buffer = null;
        length = -1;
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        // TODO: support pos/length?
        return getBinaryStream();
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CassandraBlob that = (CassandraBlob) o;

        if (buffer != null) {
            return Arrays.equals(buffer.array(), that.buffer.array());
        } else {
            return false;
        }

    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

}
