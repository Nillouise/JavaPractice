package com.taobao.tair.extend.packet.string.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.RequestPutPacket;

public class RequestPutnxPacket extends RequestPutPacket {
	
	public RequestPutnxPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_PUTNX_PACKET;
	}
	
	public RequestPutnxPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_PUTNX_PACKET;
	}
	

	public void setNamespace(short namespace) {
		// Do nothing
		this.namespace = namespace;
	}

	public void setVersion(short version) {
		// Do nothing
		this.version = version;
	}

	public void setExpire(int expire) {
		// Do nothing
		this.expired = expire;
	}

	public void setKey(Object key) {
		// Do nothing
		this.key = key;
	}
}
