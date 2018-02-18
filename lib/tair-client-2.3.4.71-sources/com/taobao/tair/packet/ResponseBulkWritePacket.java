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

public class ResponseBulkWritePacket extends BasePacket {
    private int    seq = 0;
    private int    code = 0;

    public ResponseBulkWritePacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_BULK_WRITE_PACKET;
    }

    public int encode() {
        int capacity = 4;
        writePacketBegin(capacity);
        byteBuffer.putInt(this.code);
        byteBuffer.putInt(this.seq);
        writePacketEnd();

        return 0;
    }

    public boolean decode() {
        this.code          = byteBuffer.getInt();
        this.seq           = byteBuffer.getInt();
        return true;
    }

    public int getCode() {
        return code;
    }

    public int getSequnce() {
        return seq;
    }
}
