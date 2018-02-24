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
import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseGetRangePacket extends BasePacket {
    protected int             configVersion;
    protected List<DataEntry> entryList;
    protected int             resultCode;
    protected short           flag;
    protected List<DataEntry> proxiedKeyList;
    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseGetRangePacket.class);

    public ResponseGetRangePacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_GET_RANGE_PACKET;
    }

    public ResponseGetRangePacket(Transcoder transcoder, int pcode) {
        super(transcoder);
        this.pcode = pcode;
    }

    /**
     * encode
     */
    public int encode() {
        throw new UnsupportedOperationException();
    }

    /**
     * decode
     */

    public boolean decode() {
        this.configVersion = byteBuffer.getInt();
        resultCode = byteBuffer.getInt();
        short cmd = byteBuffer.getShort();
        int count = byteBuffer.getInt();
        flag = byteBuffer.getShort() ;
        int size = 0;
        Object key = null;
        Object value = null;

        this.entryList = new ArrayList<DataEntry>(count);
        if (cmd == TairConstant.CMD_RANGE_ALL || 
          cmd == TairConstant.CMD_RANGE_ALL_REVERSE) {
            count /= 2; 
        }
        for (int i = 0; i < count; i++) {
            DataEntry de = new DataEntry();
            if (cmd == TairConstant.CMD_RANGE_ALL ||cmd == TairConstant.CMD_RANGE_ALL_REVERSE 
              ||cmd == TairConstant.CMD_RANGE_KEY_ONLY || cmd == TairConstant.CMD_RANGE_KEY_ONLY_REVERSE
              ||cmd == TairConstant.CMD_DEL_RANGE || cmd == TairConstant.CMD_DEL_RANGE_REVERSE){
                removeMetas();
                de.decodeMeta(byteBuffer);

                int msize = byteBuffer.getInt();
                size = (msize & 0x3FFFFF);
                int prefixSize = (msize >> 22);

                if (size > 0) {
                    try {
                        key = transcoder.decode(byteBuffer.array(), byteBuffer
                                .position(), size, prefixSize);
                        byteBuffer.position(byteBuffer.position() + size);
                    } catch (Throwable e) {
                        LOGGER.error("deocde key error: key_num:" + i + "size:" +size+" add:"+value+" rc:"+resultCode);
                        resultCode = ResultCode.SERIALIZEERROR.getCode();
                    }
                }
                de.setKey(key);
            }

            if (cmd == TairConstant.CMD_RANGE_ALL || cmd == TairConstant.CMD_RANGE_VALUE_ONLY
              ||cmd == TairConstant.CMD_RANGE_ALL_REVERSE || cmd == TairConstant.CMD_RANGE_VALUE_ONLY_REVERSE){
                removeMetas();
                if (cmd == TairConstant.CMD_RANGE_VALUE_ONLY){
                  de.decodeMeta(byteBuffer);
                } else { 
                  new DataEntry().decodeMeta(byteBuffer); 
                }
                // data
                size = byteBuffer.getInt();

                if (size > 0) {
                    try {
                        value = transcoder.decode(byteBuffer.array(), byteBuffer
                                .position(), size);
                    } catch (Throwable e) {
                        LOGGER.error("deocde value error: key_num:" + i + "size:" +size+" pos:"+byteBuffer.position()+" rc:"+resultCode);
                        resultCode = ResultCode.SERIALIZEERROR.getCode();
                    }
                    byteBuffer.position(byteBuffer.position() + size);
                }
                de.setValue(value);
            }

            this.entryList.add(de);
        }

        if (count > 1) {
            int pc = byteBuffer.getInt();
            if (pc > 0) {
                proxiedKeyList = new ArrayList<DataEntry>(pc);
                for (int i = 0; i < pc; i++) {
                    removeMetas();
                    DataEntry de = new DataEntry();
                    de.decodeMeta(byteBuffer);
                    size = byteBuffer.getInt();
                    if (size > 0)
                        proxiedKeyList.indexOf(transcoder.decode(byteBuffer.array(), byteBuffer
                            .position(), size));
                    byteBuffer.position(byteBuffer.position() + size);
                }
            }
        }

        return true;
    }

    public List<DataEntry> getEntryList() {
        return entryList;
    }

    public void setEntryList(List<DataEntry> entryList) {
        this.entryList = entryList;
    }

    public short getFlag(){
        return flag;
    }

    /**
     * 
     * @return the configVersion
     */
    public int getConfigVersion() {
        return configVersion;
    }

    /**
     * 
     * @param configVersion the configVersion to setSync
     */
    public void setConfigVersion(int configVersion) {
        this.configVersion = configVersion;
    }

    public int getResultCode() {
        return resultCode;
    }
}
