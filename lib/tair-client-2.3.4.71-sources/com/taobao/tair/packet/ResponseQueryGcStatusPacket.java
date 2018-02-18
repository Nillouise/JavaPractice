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

public class ResponseQueryGcStatusPacket extends BasePacket {
    private int    seq = 0;
    private int    code = 0;

    public ResponseQueryGcStatusPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_QUERY_GC_STATUS_PACKET;
    }

    public int encode() {
        int capacity = 4;
        writePacketBegin(capacity);
        byteBuffer.putInt(this.code);
        writePacketEnd();
        return 0;
    }

    public boolean decode() {
        this.code          = byteBuffer.getInt();
        return true;
    }

    public int getCode() {
        return code;
    }
}
