package com.taobao.tair.extend.packet.common.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestExistsPacket extends RequestTTLPacket {
	public RequestExistsPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_EXISTS_PACKET;
	}
	
	public RequestExistsPacket() {
		pcode = TairConstant.TAIR_REQ_EXISTS_PACKET;
	}
}
