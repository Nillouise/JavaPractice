package com.taobao.tair.packet.stat;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class FlowCheck extends BasePacket {
	int ns;
	
	public FlowCheck(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_FLOW_CHECK;
	}

	
	public int encode() {
        writePacketBegin(0);
		byteBuffer.putInt(ns);
        writePacketEnd();
		return 0;
	}
	
	public void setNamespace(int ns) {
		this.ns = ns;
	}
}
