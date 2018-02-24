/**
 * (C) 2007-2013 Taobao Inc.
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
import com.taobao.tair.etc.MixedKey;

public class RequestIncDecBoundedPacket extends BasePacket {
    private short namespace = 0;
    private int count = 1;
    private int initValue = 0;
    private int expireTime = 0;
    private Object key = null;
    private int lowBound = Integer.MIN_VALUE;
    private int upperBound = Integer.MAX_VALUE;

    public RequestIncDecBoundedPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_INC_DEC_BOUNDED_PACKET;
    }

    /**
     * encode
     */
    public int encode() {
        byte[] keyByte;
        try {
            keyByte = transcoder.encode(this.key);
        } catch (Throwable e) {
            return 3;
        }

        if (keyByte.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
            return 1;
        }

        writePacketBegin(keyByte.length + 40);

        // body
        byteBuffer.put((byte) 0);
        byteBuffer.putShort(namespace);
        byteBuffer.putInt(count);
        byteBuffer.putInt(initValue);
        byteBuffer.putInt(expireTime);

        int len = keyByte.length;
        if (key instanceof MixedKey) {
            MixedKey mixedKey = (MixedKey)key;
            int prefixSize = mixedKey.getPrefixSize();
            prefixSize <<= 22;
            len |= prefixSize;
        }
        fillMetas();
        DataEntry.encodeMeta(byteBuffer);
        byteBuffer.putInt(len);
        byteBuffer.put(keyByte);

        byteBuffer.putInt(lowBound);
        byteBuffer.putInt(upperBound);
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
     * @return the count
     */
    public int getCount() {
        return count;
    }
    

    /**
     * @param count
     *            the count to setSync
     */
    public void setCount(int count) {
        this.count = count;
    }

    /**
     * @return the initValue
     */
    public int getInitValue() {
        return initValue;
    }

    /**
     * @param initValue
     *            the initValue to setSync
     */
    public void setInitValue(int initValue) {
        this.initValue = initValue;
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

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }
    
    public void setLowBound(int lowBound) {
    	this.lowBound = lowBound;
    }
    
    public int getLowBound() {
    	return lowBound;
    }

    public void setUpperBound(int upperBound) {
    	this.upperBound = upperBound;
    }
    
    public int getUpperBound() {
    	return upperBound;
    }
}
