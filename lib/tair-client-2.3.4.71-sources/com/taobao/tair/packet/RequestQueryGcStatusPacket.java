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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;

public class RequestQueryGcStatusPacket extends BasePacket {
    int namespace;

    public RequestQueryGcStatusPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_QUERY_GC_STATUS_PACKET;
    }

    /**
     * encode
     */
    public int encode() {
        int capacity = 4;
        writePacketBegin(capacity);
        byteBuffer.putInt(namespace);
        writePacketEnd();
        return 0;
    }

    /**
     * decode
     */
    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    public void setNamespace(int namespace) {
        this.namespace = namespace;
    }
}
