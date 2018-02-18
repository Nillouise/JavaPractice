package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestHideByProxyPacket extends RequestInvalidPacket {
	
	public RequestHideByProxyPacket(Transcoder transcoder, String groupName) {
		super(transcoder, groupName);
		this.pcode = TairConstant.TAIR_REQ_HIDE_BY_PROXY_PACKET;
	}
}
