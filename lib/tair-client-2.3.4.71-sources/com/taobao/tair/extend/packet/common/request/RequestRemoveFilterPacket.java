package com.taobao.tair.extend.packet.common.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestRemoveFilterPacket extends RequestAddFilterPacket {
	public RequestRemoveFilterPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_REMOVE_FILTER_PACKET;
	}

	public RequestRemoveFilterPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_REMOVE_FILTER_PACKET;
	}

}
