package com.taobao.tair.extend.packet.set.response;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.extend.packet.set.response.ResponseSimplePacket;

public class ResponseSRemPacket extends ResponseSimplePacket {
	public ResponseSRemPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_RESP_SREM_PACKET;
	}
}
