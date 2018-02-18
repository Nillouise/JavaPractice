package com.taobao.tair.extend.packet.common.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class RequestDumpAreaPacket  extends BasePacket {

	private static final int HEARDER_LEN = 1 + 2;
	
	private short namespace = 0;
	
	public RequestDumpAreaPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_DUMP_AREA_PACKET;
	}

	public RequestDumpAreaPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_DUMP_AREA_PACKET;
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
