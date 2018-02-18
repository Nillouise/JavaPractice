/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponsePrefixIncDecPacket extends BasePacket {
    private int configVersion = 0;
    private int code = 0;
    private int successCount = 0;
    private int failedCount = 0;
    private Map<Object, Result<Integer>> resultMap = new HashMap<Object, Result<Integer>>();

    public ResponsePrefixIncDecPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_PREFIX_INCDEC_PACKET;
    }

    public int encode() {
        throw new UnsupportedOperationException();
    }

    public boolean decode() {
        this.configVersion = byteBuffer.getInt();
        this.code = byteBuffer.getInt();
        DataEntry de = new DataEntry();
        int size = 0;
        this.successCount = byteBuffer.getInt();
        for (int i = 0; i < this.successCount; ++i) {
            removeMetas();
            new DataEntry().decodeMeta(byteBuffer);
            Object key = null;
            size = byteBuffer.getInt();
            if (size > 0) {
                try {
                    key = transcoder.decode(byteBuffer.array(), byteBuffer.position(), size);
                } catch (Throwable e) {
                    return false;
                }
                byteBuffer.position(byteBuffer.position() + size);
                int value = byteBuffer.getInt();
                resultMap.put(key, new Result<Integer>(ResultCode.SUCCESS, value));
            }
        }
        this.failedCount = byteBuffer.getInt();
        for (int i = 0; i < this.failedCount; ++i) {
            removeMetas();
            de.decodeMeta(byteBuffer);
            size = byteBuffer.getInt();
            Object key = null;
            if (size > 0) {
                try {
                    key = transcoder.decode(byteBuffer.array(), byteBuffer.position(), size);
                } catch (Throwable e) {
                    return false;
                }
                byteBuffer.position(byteBuffer.position() + size);
                int rc = byteBuffer.getInt();
                resultMap.put(key, new Result<Integer>(ResultCode.valueOf(rc)));
            }
        }

        return true;
    }

    public int getConfigVersion() {
        return this.configVersion;
    }

    public int getCode() {
        return this.code;
    }

    public int getResultCode() {
    	return this.code;
    }

    public int getSuccessCount() {
        return this.successCount;
    }

    public int getFailedCount() {
        return this.failedCount;
    }

    public Map<Object, Result<Integer>> getResultMap() {
        return this.resultMap;
    }
}
