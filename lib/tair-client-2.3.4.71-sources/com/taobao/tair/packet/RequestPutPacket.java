/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.TairIllegalArgumentException;

public class RequestPutPacket extends BasePacket {
	protected short namespace;
	protected short version;
	protected int expired;
	protected Object key;
	protected Object data;
	protected boolean rdbSetCount;

	public RequestPutPacket() {
		super();
		pcode = TairConstant.TAIR_REQ_PUT_PACKET;
		rdbSetCount = false;
	}
	
	public RequestPutPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_PUT_PACKET;
		this.rdbSetCount = false;
	}

	/**
	 * encode
	 */
	public int encode() {
		return encode(0, 0);
	}

	/**
	 * encode with flag
	 */
	public int encode(int keyFlag, int valueFlag) {
		byte[] keyByte = null;
		byte[] dataByte = null;
		boolean keyEncoded = false;
		try {
			keyByte = transcoder.encode(key);
			keyEncoded = true;
			// if rdbSetCount, should not use the transcoder logic
			if (rdbSetCount && (data instanceof Long || data instanceof Integer || data instanceof Short)) {
				dataByte = String.valueOf(data).getBytes();
			} else {
				dataByte = transcoder.encode(data);
			}
		} catch (Throwable e) {
			if (e instanceof TairIllegalArgumentException) {
				int errCode =  ((TairIllegalArgumentException) e).getErrCode();
				return (!keyEncoded && TairConstant.VALUETOLARGE == errCode) ? TairConstant.KEYTOLARGE : errCode;
			}
			return 3; // serialize error
		}

		if (keyByte.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
			return 1;
		}

		if (dataByte.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
			return 2;
		}

		short prefixSize = 0;
		if (key instanceof MixedKey) {
			MixedKey mixedKey = (MixedKey)key;
			prefixSize = mixedKey.getPrefixSize();
		}
		writePacketBegin(keyByte.length + dataByte.length);

		int len = prefixSize;
		len <<= 22;
		len |= keyByte.length;
		// body
		byteBuffer.put((byte) 0);
		byteBuffer.putShort(namespace);
		byteBuffer.putShort(version);
		byteBuffer.putInt(expired);

		fillMetas();
		DataEntry.encodeMeta(byteBuffer, keyFlag);
		byteBuffer.putInt(len);
		byteBuffer.put(keyByte);

		fillMetas();
		DataEntry.encodeMeta(byteBuffer, valueFlag);
		byteBuffer.putInt(dataByte.length);
		byteBuffer.put(dataByte);

		writePacketEnd();

		return 0;
	}

	/**
	 * decode
	 */
	public boolean decode() {
		throw new UnsupportedOperationException();
	}

	/**
	 * @return the data
	 */
	public Object getData() {
		return data;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setData(Object data) {
		this.data = data;
	}

	/**
	 * @return the expired
	 */
	public int getExpired() {
		return expired;
	}

	/**
	 * @param expired
	 *            the expired to set
	 */
	public void setExpired(int expired) {
		this.expired = expired;
	}

	/**
	 * @return the key
	 */
	public Object getKey() {
		return key;
	}

	/**
	 * @param key
	 *            the key to set
	 */
	public void setKey(Object key) {
		this.key = key;
	}

	/**
	 * @return the namespace
	 */
	public short getNamespace() {
		return namespace;
	}

	/**
	 * @param namespace
	 *            the namespace to set
	 */
	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}

	/**
	 * @return the version
	 */
	public short getVersion() {
		return version;
	}

	/**
	 * @param version
	 *            the version to set
	 */
	public void setVersion(short version) {
		this.version = version;
	}
	
	/**
	 * @param rdbSetCount
	 *            if is invoked by rdbSetCount
	 */
	public void setRdbSetCount(boolean rdbSetCount) {
		this.rdbSetCount = rdbSetCount;
	}

}
