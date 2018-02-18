package com.taobao.tair.packet.stat;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class FlowViewRequest extends BasePacket {
	int area;
	
	public FlowViewRequest(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_FLOW_VIEW;
	}

	
	public int encode() {
        writePacketBegin(0);
		byteBuffer.putInt(area);
        writePacketEnd();
		return 0;
	}
	
	public void setArea(int area) {
		this.area = area;
	}
}
