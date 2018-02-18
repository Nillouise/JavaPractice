package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestQueryBulkWriteTokenPacket extends BasePacket {
	int namespace;

	public RequestQueryBulkWriteTokenPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_QUERY_BULK_WRITE_TOKEN_PACKET;
	}

	/**
	 * encode
	 */
	public int encode() {
		int capacity = 4;
		writePacketBegin(capacity);
		byteBuffer.putInt(namespace);
		writePacketEnd();
		return 0;
	}

	/**
	 * decode
	 */
	public boolean decode() {
		throw new UnsupportedOperationException();
	}


	public void setNamespace(int namespace) {
		this.namespace = namespace;
	}
}
