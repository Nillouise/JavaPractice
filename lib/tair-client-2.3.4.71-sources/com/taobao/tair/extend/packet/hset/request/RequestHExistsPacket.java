package com.taobao.tair.extend.packet.hset.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestHExistsPacket extends RequestHGetPacket {
	public RequestHExistsPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_HEXISTS_PACKET;
	}

	public RequestHExistsPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_HEXISTS_PACKET;
	}
}
