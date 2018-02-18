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

public class RequestRemoveArea extends BasePacket {
	int area;
	
    public RequestRemoveArea(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_REMOVE_AREA;
    }
    
    public int getArea() {
		return area;
	}

	public void setArea(int area) {
		this.area = area;
	}

	@Override
	public int encode() {
		writePacketBegin(4);
		byteBuffer.putInt(area);
		writePacketEnd();
		return 0;
	}
}
