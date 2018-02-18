package com.taobao.tair.extend.packet.string.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.extend.packet.set.request.RequestSAddPacket;

public class RequestGetSetPacket extends RequestSAddPacket {
	public RequestGetSetPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_GETSET_PACKET;
	}
	
	public RequestGetSetPacket() {
		pcode = TairConstant.TAIR_REQ_GETSET_PACKET;
	}
}
