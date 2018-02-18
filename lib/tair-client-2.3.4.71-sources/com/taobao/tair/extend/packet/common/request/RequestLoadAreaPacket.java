package com.taobao.tair.extend.packet.common.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class RequestLoadAreaPacket extends BasePacket {

	private static final int HEARDER_LEN = 1 + 2;
	
	private short namespace = 0;
	
	public RequestLoadAreaPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_LOAD_AREA_PACKET;
	}

	public RequestLoadAreaPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_LOAD_AREA_PACKET;
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
}
