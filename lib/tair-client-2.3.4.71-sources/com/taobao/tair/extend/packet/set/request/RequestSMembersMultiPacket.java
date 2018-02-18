package com.taobao.tair.extend.packet.set.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class RequestSMembersMultiPacket extends BasePacket {
	
	private final static int HEADER_LEN = 1 + 2 + 4;
	
	private short namespace = 0;
	private Set<Serializable> keys = null;
	
	private List<byte[]> keysbytes = new ArrayList<byte[]>();
	
	public RequestSMembersMultiPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_SMEMBERSMULTI_PACKET;
	}

	public RequestSMembersMultiPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_SMEMBERSMULTI_PACKET;
	}

	public int encode() {
		if (keys == null || keys.size() <= 0) {
			return TairConstant.SERIALIZEERROR;
		}
		
		byte[] keybytes = null;
		
		int keyslen = 0;
		for(Serializable okey : keys) {
			if(okey == null) {
				return TairConstant.SERIALIZEERROR;
			}
			try {
				keybytes = transcoder.encode(okey);
			} catch (Throwable e) {
				return TairConstant.SERIALIZEERROR;
			}
			if (keybytes == null) {
				return TairConstant.SERIALIZEERROR;
			}
			if (keybytes.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
				return TairConstant.VALUETOLARGE;
			}
			keysbytes.add(keybytes);
			keyslen += keybytes.length;
		}
		
		writePacketBegin(HEADER_LEN + keyslen + 4*keys.size());
		byteBuffer.put((byte)0);
		byteBuffer.putShort(namespace);
		byteBuffer.putInt(keysbytes.size());
		for(byte[] tkeybytes : keysbytes) {
			byteBuffer.putInt(tkeybytes.length);
			byteBuffer.put(tkeybytes);
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

	public void setKeys(Set<Serializable> keys) {
		this.keys = keys;
	}
	
	public Set<Serializable> getKeys() {
		return this.keys;
	}
}
