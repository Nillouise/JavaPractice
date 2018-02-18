/**
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.Map;
import java.util.HashMap;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.ResultCode;
import com.taobao.tair.DataEntry;

public class MReturnPacket extends BasePacket {
    private int configVersion = 0;
    private int code = 0;
    private String msg = null; //~ share one single message
    private int keyCount = 0;
    private Map<Object, ResultCode> keyCodeMap = null;

    public MReturnPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_MRETURN_PACKET;
    }

    public int encode() {
        throw new UnsupportedOperationException();
    }

    public boolean decode() {
        this.configVersion = byteBuffer.getInt();
        this.code = byteBuffer.getInt();
        this.msg = readString();
        this.keyCount = byteBuffer.getInt();

        if (keyCount > 0) {
            keyCodeMap = new HashMap<Object, ResultCode>();
            DataEntry de = new DataEntry();
            for (int i = 0; i < keyCount; ++i) {
                removeMetas();
                de.decodeMeta(byteBuffer);
                int size = byteBuffer.getInt();
                Object key = null;
                if (size > 0) {
                    try {
                        key = transcoder.decode(byteBuffer.array(), byteBuffer
                                .position(), size);
                    } catch (Throwable e) {
                        return false;
                    }
                    byteBuffer.position(byteBuffer.position() + size);
                    int rc = byteBuffer.getInt();
                    keyCodeMap.put(key, ResultCode.valueOf(rc));
                }
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

    public String getMsg() {
        return this.msg;
    }

    public int getKeyCount() {
        return this.keyCount;
    }

    /**
     * @return keyCodeMap: indicating the result code for each failed key
     */
    public Map<Object, ResultCode> getKeyCodeMap() {
        return this.keyCodeMap;
    }

    public int getResultCode() {
        return this.code;
    }
}
