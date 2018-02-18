package com.taobao.tair.extend.packet.common.request;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class RequestAddFilterPacket extends BasePacket {

	private final static int HEADER_LEN = 1 + 2 + 4 + 4 + 4;
	
	private short namespace = 0;
	
	private Object keyPat = null;
	private Object fieldPat = null; 
	private Object valuePat = null;
	
	public RequestAddFilterPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_ADD_FILTER_PACKET;
	}

	public RequestAddFilterPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_ADD_FILTER_PACKET;
	}

	public int encode() {
		byte[] keybytes = null;
		byte[] fieldbytes = null;
		byte[] valuebytes = null;
		if (keyPat == null && fieldPat == null && valuePat == null) {
			return TairConstant.SERIALIZEERROR;
		}
		try {
			if (keyPat != null) {
				keybytes = transcoder.encode(keyPat);
			}
			if (fieldPat != null) {
				fieldbytes = transcoder.encode(fieldPat);
			}
			if (valuePat != null) {
				valuebytes = transcoder.encode(valuePat);
			}
		} catch (Throwable e) {
			return TairConstant.SERIALIZEERROR;
		}
		if (keybytes == null && valuebytes == null && fieldbytes == null) {
			return TairConstant.SERIALIZEERROR;
		}
		
		if (keybytes != null && keybytes.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
			return TairConstant.KEYTOLARGE;
		}
	    if (valuebytes != null && valuebytes.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
			return TairConstant.VALUETOLARGE;
		}
		if (fieldbytes != null && fieldbytes.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
			return TairConstant.VALUETOLARGE;
		}
		
	    int klen = (keybytes == null ? 0 : keybytes.length);
	    int flen = (fieldbytes == null ? 0 : fieldbytes.length);
	    int vlen = (valuebytes == null ? 0 : valuebytes.length);
		writePacketBegin(HEADER_LEN + klen + flen + vlen);
		byteBuffer.put((byte)0);
		byteBuffer.putShort(namespace);
		if (klen == 0) {
			byteBuffer.putInt(0);
		} else {
			byteBuffer.putInt(keybytes.length);
			byteBuffer.put(keybytes);
		}
		if (flen == 0) {
			byteBuffer.putInt(0);
		} else {
			byteBuffer.putInt(fieldbytes.length);
			byteBuffer.put(fieldbytes);
		}
		if (vlen == 0) {
			byteBuffer.putInt(0);
		} else {
			byteBuffer.putInt(valuebytes.length);
			byteBuffer.put(valuebytes);
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
	
	public void setKeyPat(Object keyPat) {
		this.keyPat = keyPat;
	}
	public Object getKeyPat() {
		return this.keyPat;
	}
	
	public void setFieldPat(Object fieldPat) {
		this.fieldPat = fieldPat;
	}
	public Object getFieldPat() {
		return this.fieldPat;
	}
	
	public void setValuePat(Object valuePat) {
		this.valuePat = valuePat;
	}
	public Object getValuePat() {
		return this.fieldPat;
	}
}
