package com.taobao.tair.extend.packet.set.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestSRemMultiPacket extends RequestSAddMultiPacket {

	public RequestSRemMultiPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_SREMMULTI_PACKET;
	}
	
	public RequestSRemMultiPacket() {
		pcode = TairConstant.TAIR_REQ_SREMMULTI_PACKET;
	}
}
