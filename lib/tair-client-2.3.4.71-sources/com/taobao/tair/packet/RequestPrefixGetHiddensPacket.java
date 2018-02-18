package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestPrefixGetHiddensPacket extends RequestGetPacket {

	public RequestPrefixGetHiddensPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_PREFIX_GET_HIDDENS_PACKET;
	}

}
