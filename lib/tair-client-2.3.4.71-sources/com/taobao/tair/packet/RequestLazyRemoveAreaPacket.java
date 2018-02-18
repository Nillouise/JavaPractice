package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestLazyRemoveAreaPacket  extends BasePacket {

	private static final int HEARDER_LEN = 1 + 2;
	
	private short namespace = 0;
	private String password = null;
	
	public RequestLazyRemoveAreaPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_LAZY_REMOVE_AREA_PACKET;
	}

	public RequestLazyRemoveAreaPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_LAZY_REMOVE_AREA_PACKET;
	}

	public int encode() {
		writePacketBegin(HEARDER_LEN);
		byteBuffer.put((byte)0);         //1
		byteBuffer.putShort(namespace);  //2
		
		writePacketEnd();
		
		return 0;
	}
	
	public boolean decode() {
		throw new UnsupportedOperationException();
	}
	
	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}
	public short getNamespace() {
		return this.namespace;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	public String getPassword() {
		return this.password;
	}
}
