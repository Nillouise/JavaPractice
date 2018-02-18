/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestBulkWritePacket extends BasePacket {
    int token;
    int namespace;
    int bucket;
    int keyCount;
    int size;
    byte[] buf;

    public RequestBulkWritePacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_BULK_WRITE_PACKET;
    }

    /**
     * encode
     */
    public int encode() {
        int capacity = 0;
        capacity = 20 + size;
        writePacketBegin(capacity);
        byteBuffer.putInt(token);
        byteBuffer.putInt(namespace);
        byteBuffer.putInt(bucket);
        byteBuffer.putInt(keyCount);
        byteBuffer.putInt(size);
        byteBuffer.put(buf, 0, size);
        writePacketEnd();
        return 0;
    }

    /**
     * decode
     */
    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    public void setKeyCount(int count) {
        this.keyCount = count;
    }

    public void setToken(int token) {
        this.token = token;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public void setNamespace(int namespace) {
        this.namespace = namespace;
    }

    public void setFileContent(byte[] buf) {
        this.buf = buf;
    }

    public void setFileSize(int size) {
        this.size = size;
    }
}
