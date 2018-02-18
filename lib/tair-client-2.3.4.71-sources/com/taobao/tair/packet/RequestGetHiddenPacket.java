package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestGetHiddenPacket extends RequestGetPacket {

	public RequestGetHiddenPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_GET_HIDDEN_PACKET;
	}

}