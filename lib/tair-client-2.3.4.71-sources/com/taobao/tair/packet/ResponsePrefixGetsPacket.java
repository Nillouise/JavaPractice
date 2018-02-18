/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.Map;
import java.util.HashMap;

import com.taobao.tair.DataEntry;
import com.taobao.tair.ResultCode;
import com.taobao.tair.Result;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.comm.Transcoder;

public class ResponsePrefixGetsPacket extends BasePacket {
    private int configVersion = 0;
    private int code = 0;
    private Object pkey = null;
    private Map<Object, Result<DataEntry>> entryMap = null;
    private int successCount = 0;
    private int failedCount = 0;

    public ResponsePrefixGetsPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_PREFIX_GETS_PACKET;
    }

    public int encode() {
        throw new UnsupportedOperationException();
    }

    public boolean decode() {
        this.configVersion = byteBuffer.getInt();
        this.code = byteBuffer.getInt();
        int size = 0;
        Object key = null;
        Object value = null;

        int prefixKeySize = 0;
        removeMetas();
        new DataEntry().decodeMeta(byteBuffer);
        size = byteBuffer.getInt();
        if (size > 0) {
            try {
                //~ omit the two leading bytes indicating the MixedKey type
                pkey = transcoder.decode(byteBuffer.array(), byteBuffer.position()+2, size-2);
                byteBuffer.position(byteBuffer.position() + size);
            } catch (Throwable e) {
                return false;
            }
        }

        prefixKeySize = size;
        entryMap = new HashMap<Object, Result<DataEntry>>();
        this.successCount = byteBuffer.getInt();
        if (this.successCount > 0) {
            for (int i = 0; i < successCount; ++i) {
                DataEntry de = new DataEntry();
                removeMetas();
                de.decodeMeta(byteBuffer);
                size = byteBuffer.getInt();
                if (size > 0) {
                    try {
                        key = transcoder.decode(byteBuffer.array(), byteBuffer.position(), size);
                        byteBuffer.position(byteBuffer.position() + size);
                    } catch (Throwable e) {
                        return false;
                    }
                    de.setKey(key);
                }

                removeMetas();
                new DataEntry().decodeMeta(byteBuffer);
                size = byteBuffer.getInt();
                if (size > 0) {
                    try {
                        value = transcoder.decode(byteBuffer.array(), byteBuffer.position(), size);
                        byteBuffer.position(byteBuffer.position() + size);
                    } catch (Throwable e) {
                        return false;
                    }
                    de.setValue(value);
                }
                Result<DataEntry> result = new Result<DataEntry>(ResultCode.valueOf(byteBuffer.getInt()), de);
                de.setPrefixKeySize(prefixKeySize);
                entryMap.put(de.getKey(), result);
            }
        }
        this.failedCount = byteBuffer.getInt();
        if (this.failedCount > 0) {
            for (int i = 0; i < this.failedCount; ++i) {
                DataEntry de = new DataEntry();
                removeMetas();
                de.decodeMeta(byteBuffer);
                size = byteBuffer.getInt();
                if (size > 0) {
                    try {
                        key = transcoder.decode(byteBuffer.array(), byteBuffer.position(), size);
                        byteBuffer.position(byteBuffer.position() + size);
                    } catch (Throwable e) {
                        return false;
                    }
                    de.setKey(key);
                }
                int ecode = byteBuffer.getInt();
                ResultCode rc = ResultCode.valueOf(ecode);
                de.setPrefixKeySize(prefixKeySize);
                Result<DataEntry> result = new Result<DataEntry>(rc, de);
                entryMap.put(de.getKey(), result);
            }
        }
        return true;
    }

    public int getConfigVersion() {
        return this.configVersion;
    }

    public int getResultCode() {
        return this.code;
    }

    public Object getPKey() {
        return this.pkey;
    }

    public Map<Object, Result<DataEntry>> getEntryMap() {
        return this.entryMap;
    }

    public int getSuccessCount() {
        return this.successCount;
    }

    public int getFailedCount() {
        return this.failedCount;
    }
}
