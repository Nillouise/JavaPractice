/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestPutModifyDatePacket extends BasePacket {
    protected short  namespace;
    protected short  version;
    protected int    expired;
    protected long   modifyTime;
    protected Object key;
    protected Object data;

    public RequestPutModifyDatePacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_PUT_MODIFY_DATE_PACKET;
    }

    /**
     * encode
     */
    public int encode() {
        byte[] keyByte = null;
        byte[] dataByte = null;
        try {
            keyByte = transcoder.encode(key);
            dataByte = transcoder.encode(data);
        } catch (Throwable e) {
            return 3; // serialize error
        }

        if (keyByte.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
            return 1;
        }

        if (dataByte.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
            return 2;
        }

        writePacketBegin(keyByte.length + dataByte.length);

        // body
        byteBuffer.put((byte) 0);
        byteBuffer.putShort(namespace);
        byteBuffer.putShort(version);
        byteBuffer.putInt(expired);
        byteBuffer.putLong(modifyTime);

        fillMetas();
        DataEntry.encodeMeta(byteBuffer);
        byteBuffer.putInt(keyByte.length);
        byteBuffer.put(keyByte);

        fillMetas();
        DataEntry.encodeMeta(byteBuffer);
        byteBuffer.putInt(dataByte.length);
        byteBuffer.put(dataByte);

        writePacketEnd();

        return 0;
    }

    /**
     * decode
     */
    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the data
     */
    public Object getData() {
        return data;
    }

    /**
     * @param data
     *            the data to setSync
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * @return the expired
     */
    public int getExpired() {
        return expired;
    }

    /**
     * @param expired
     *            the expired to setSync
     */
    public void setExpired(int expired) {
        this.expired = expired;
    }

    /**
     * @return the key
     */
    public Object getKey() {
        return key;
    }

    /**
     * @param key
     *            the key to setSync
     */
    public void setKey(Object key) {
        this.key = key;
    }

    /**
     * @return the namespace
     */
    public short getNamespace() {
        return namespace;
    }

    /**
     * @param namespace
     *            the namespace to setSync
     */
    public void setNamespace(short namespace) {
        this.namespace = namespace;
    }

    /**
     * @return the version
     */
    public short getVersion() {
        return version;
    }

    /**
     * @param version
     *            the version to setSync
     */
    public void setVersion(short version) {
        this.version = version;
    }

    public double getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(long modifyTime) {
        this.modifyTime = modifyTime;
    }

}
