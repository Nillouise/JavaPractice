/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;

public class RequestGetRangePacket extends BasePacket {
    protected short        cmdType;
    protected short        namespace;
    protected List<Object> keyList     = new ArrayList<Object>();
    protected int          offset;
    protected int          limit;

    private int            prefix_size = 0;

    public int getCmd() {
      return cmdType;
    }
    public void setCmd(short cmd) {
       cmdType = cmd;
    }

    public int getPrefix_size() {
        return prefix_size;
    }

    public void setPrefix_size(int prefixSize) {
        prefix_size = prefixSize;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public RequestGetRangePacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_GET_RANGE_PACKET;
    }

    /**
     * encode
     */
    public int encode() {
        int capacity = 13;
        List<byte[]> list = new ArrayList<byte[]>();

        if ( limit < 0 || offset <0 || keyList.size() != 2 ){
            return 4; 
        }

        for (Object key : keyList) {
            if (key instanceof MixedKey){
                byte[] keyByte = transcoder.encode(key);
                int size= ((MixedKey)key).getPrefixSize();

                if (keyByte.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
                    return 1;
                }
                if (prefix_size == 0){
                    prefix_size = size ;
                } else if (prefix_size != size){ //endKey has different prefix with startKey
                    return 3;
                }

                list.add(keyByte);
                capacity += 40;
                capacity += keyByte.length;
            } else {
                return 3;
            }
        }

        writePacketBegin(capacity);

        // body
        byteBuffer.put((byte) 0);
        byteBuffer.putShort(cmdType);
        byteBuffer.putShort(namespace);
        byteBuffer.putInt(offset);
        byteBuffer.putInt(limit); //13

        for (byte[] keyByte : list) {
            fillMetas(); //7
            DataEntry.encodeMeta(byteBuffer); //29
            byteBuffer.putInt((prefix_size << TairConstant.PREFIX_KEY_OFFSET) | keyByte.length); //4
            byteBuffer.put(keyByte);
        }
        writePacketEnd();

        return 0;
    }

    /**
     * decode
     */
    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    public boolean setStartKey(Object key) {
        return this.keyList.add(key);
    }

    public boolean setEndKey(Object key) {
        return this.keyList.add(key);
    }

    /**
     * 
     * @return the keyList
     */
    public List<Object> getKeyList() {
        return keyList;
    }

    /**
     * 
     * @param keyList
     *            the keyList to set
     */
    public void setKeyList(List<Object> keyList) {
        this.keyList = keyList;
    }

    /**
     * 
     * @return the namespace
     */
    public short getNamespace() {
        return namespace;
    }

    /**
     * 
     * @param namespace
     *            the namespace to set
     */
    public void setNamespace(short namespace) {
        this.namespace = namespace;
    }

}
