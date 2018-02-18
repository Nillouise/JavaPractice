package com.taobao.tair.extend.packet.list.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.extend.packet.LeftOrRight;

public class RequestLRPushLimitPacket extends RequestLRPushPacket {
	private final static int HEADER_LEN = 1 + 2 + 2 + 4 + 4 + 4 + 4;
	private int maxCount = 0;
	
	public RequestLRPushLimitPacket(Transcoder transcoder, LeftOrRight lr) {
		super(transcoder, lr);
		if (lr == LeftOrRight.IS_L)
			this.pcode = TairConstant.TAIR_REQ_LPUSH_LIMIT_PACKET;
		else
			this.pcode = TairConstant.TAIR_REQ_RPUSH_LIMIT_PACKET;
	}
	public RequestLRPushLimitPacket() {
	}
	
	public void setLeftOrRight(LeftOrRight lr) {
		if (lr == LeftOrRight.IS_L)
			this.pcode = TairConstant.TAIR_REQ_LPUSH_LIMIT_PACKET;
		else
			this.pcode = TairConstant.TAIR_REQ_RPUSH_LIMIT_PACKET;
	}
	
	public int encode() {	
		byte[] keybytes = null;
		byte[] valuebytes = null;
		if (key == null || values.size() == 0) {
			return TairConstant.SERIALIZEERROR;
		}
		try {
			keybytes = transcoder.encode(key);
		} catch (Throwable e) {
			return TairConstant.SERIALIZEERROR;
		}
		if(keybytes == null) {
			return TairConstant.SERIALIZEERROR;
		}
		
		if(keybytes.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
			return TairConstant.KEYTOLARGE;
		}
		
		int valueslen = 0;
		for (Object value : values) {
			if (value == null) {
				return TairConstant.SERIALIZEERROR;
			}
			try {
				valuebytes = transcoder.encode(value);
			} catch (Throwable e) {
				return TairConstant.SERIALIZEERROR;
			}
			if (valuebytes == null) {
				return TairConstant.SERIALIZEERROR;
			}
			if(valuebytes.length > 1024*1024) {
				return TairConstant.SERIALIZEERROR;
			}
			
			if (valuebytes.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
				return TairConstant.VALUETOLARGE;
			}
			
			bytevalues.add(valuebytes);
			valueslen += valuebytes.length;
		}
		
		writePacketBegin(HEADER_LEN + keybytes.length + valueslen + 4 * bytevalues.size());
		byteBuffer.put((byte)0);
		byteBuffer.putShort(namespace);
		byteBuffer.putShort((short)version);
		byteBuffer.putInt(expire);
		byteBuffer.putInt(maxCount);
		byteBuffer.putInt(keybytes.length);
		byteBuffer.put(keybytes);
		byteBuffer.putInt(bytevalues.size());
		for (byte[] vb : bytevalues) {
			byteBuffer.putInt(vb.length);
			byteBuffer.put(vb);
		}
		writePacketEnd();
		
		return 0;
	}
	
	public void setMaxCount(int maxcount) {
		maxCount = maxcount;
	}
}
