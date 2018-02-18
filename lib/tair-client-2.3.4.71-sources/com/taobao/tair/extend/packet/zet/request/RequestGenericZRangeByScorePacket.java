package com.taobao.tair.extend.packet.zet.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.helper.BytesHelper;
import com.taobao.tair.packet.BasePacket;

public class RequestGenericZRangeByScorePacket extends BasePacket {
	
	private final static int HEADER_LEN = 1 + 2 + 4 + 4 + 4 + 4 + 8 + 8;
	
	private short namespace = 0;
	private Object key = null;
	private double start = 0;
	private double end = 0;
	private boolean reverse = false;
	private int limit = -1;
	private boolean withscore = false;
	
	public RequestGenericZRangeByScorePacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET;
	}

	public RequestGenericZRangeByScorePacket() {
		super();
		pcode = TairConstant.TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET;
	}

	public int encode() {
		byte[] keybytes = null;
		if (key == null) {
			return TairConstant.SERIALIZEERROR;
		}
		try {
			keybytes = transcoder.encode(key);
		} catch (Throwable e) {
			return TairConstant.SERIALIZEERROR;
		}
		if (keybytes == null) {
			return TairConstant.SERIALIZEERROR;
		}
		
		if(keybytes.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
			return TairConstant.KEYTOLARGE;
		}
		
		writePacketBegin(HEADER_LEN + keybytes.length);
		byteBuffer.put((byte)0);
		byteBuffer.putShort(namespace);
		byteBuffer.put(BytesHelper.DoubleToBytes_With_Little_Endian(start));
		byteBuffer.put(BytesHelper.DoubleToBytes_With_Little_Endian(end));
		byteBuffer.putInt(keybytes.length);
		byteBuffer.put(keybytes);
		if (reverse) {
			byteBuffer.putInt(1);
		} else {
			byteBuffer.putInt(0);
		}
		byteBuffer.putInt(limit);
		if (withscore) {
			byteBuffer.putInt(1);
		} else {
			byteBuffer.putInt(0);
		}
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
	
	public void setKey(Object key) {
		this.key = key;
	}
	public Object getKey() {
		return this.key;
	}
	
	public void setStartScore(double start) {
		this.start = start;
	}
	public double getStartScore() {
		return this.start;
	}
	
	public void setEndScore(double end) {
		this.end = end;
	}
	public double getEndScore() {
		return this.end;
	}
	
	public void setLimit(int limit) {
		this.limit = limit;
	}
	public int getLimit() {
		return this.limit;
	}
	
	public void setReverse(boolean reverse) {
		this.reverse = reverse;
	}
	public boolean getReverse() {
		return this.reverse;
	}
	
	public void setWithScore(boolean withscore) {
		this.withscore = withscore;
	}
	public boolean getWithScore() {
		return this.withscore;
	}
}
