package com.taobao.tair.extend.packet.string.response;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.extend.packet.hset.response.ResponseHGetPacket;

public class ResponseGetSetPacket extends ResponseHGetPacket {
	public ResponseGetSetPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_RESP_GETSET_PACKET;
	}
}
