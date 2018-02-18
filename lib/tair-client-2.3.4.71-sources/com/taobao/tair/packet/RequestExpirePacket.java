package com.taobao.tair.packet;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class RequestExpirePacket extends BasePacket {
    private short namespace = 0;
    private Object key = null;
    private int expiretime = 0;

    public RequestExpirePacket(Transcoder transcoder) {
	super(transcoder);
	pcode = TairConstant.TAIR_REQ_EXPIRE_PACKET;
    }

    public RequestExpirePacket() {
	super();
	pcode = TairConstant.TAIR_REQ_EXPIRE_PACKET;
    }

    public int encode() {
	byte[] keyByte = null;
	try {
	    keyByte = transcoder.encode(key);
	} catch (Throwable e) {
	    return 3; // serialize error
	}

	if (keyByte.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
	    return 1;
	}

	short prefixSize = 0;
	if (key instanceof MixedKey) {
	    MixedKey mixedKey = (MixedKey) key;
	    prefixSize = mixedKey.getPrefixSize();
	}
	writePacketBegin(keyByte.length);

	int len = prefixSize;
	len <<= 22;
	len |= keyByte.length;
	// body
	byteBuffer.put((byte) 0);
	byteBuffer.putShort(namespace);
	byteBuffer.putInt(expiretime);

	fillMetas();
	DataEntry.encodeMeta(byteBuffer, 0);
	byteBuffer.putInt(len);
	byteBuffer.put(keyByte);

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

    public void setExpire(int expiretime) {
	this.expiretime = expiretime;
    }

    public int getExpire() {
	return this.expiretime;
    }
}
