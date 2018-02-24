/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.taobao.tair.etc.TairUtil;

/**
 * data entry object, includes key, value and meta infomations
 */
public class DataEntry extends DataEntryAbstract<Object> implements Serializable {

	private static final long serialVersionUID = -3492001385938512871L;
	private static byte[] DEFAULT_DATA = new byte[29];
	static {
		Arrays.fill(DEFAULT_DATA, (byte)0);
	}
	private Object key;

	// meta data

	private int magic = 0;
	private int checkSum = 0;
	private int keySize = 0;
	private int version = 0;
	private int padSize = 0;
	private int valueSize = 0;
	private int flag = 0;
	private int cdate = 0;
	private int mdate = 0;
	private int edate = 0;
	private int prefixKeySize = 0;

	// setSync fill cache flag, just trick flag, meaningless to real data meta flag
	public static final int TAIR_CLIENT_PUT_FILL_CACHE_FLAG = 0;
	public static final int TAIR_CLIENT_PUT_SKIP_CACHE_FLAG = 1;

	// meta flag
	public static final int TAIR_ITEM_FLAG_ADDCOUNT = 1;
	public static final int TAIR_ITEM_LOCKED = 8;
	public static final int TAIR_ITEM_HIDDEN = 2;
	public static final int TAIR_ITEM_NEGLECTED = 4;
	public static final int TAIR_ITEM_COUNTER = 1;

	public boolean isLocked() {
		return (flag & TAIR_ITEM_LOCKED) != 0;
	}
	public boolean isHidden() {
		return (flag & TAIR_ITEM_HIDDEN) != 0;
	}
	public boolean isNeglected() {
		return (flag & TAIR_ITEM_NEGLECTED) != 0;
	}
	public boolean isCounter() {
		return (flag & TAIR_ITEM_COUNTER) != 0;
	}

	public int getExpriedDate() {
		return edate;
	}

    public void setExpiredDate(int edate) {
        this.edate = edate;
    }

	public int getCreateDate() {
		return cdate;
	}

	public int getModifyDate() {
		return mdate;
	}

	public void setPrefixKeySize(int prefixKeySize) {
		this.prefixKeySize = prefixKeySize;
	}
	public int getPrefixKeySize() {
		return this.prefixKeySize;
	}
	public DataEntry() {
	}

	public DataEntry(Object value) {
		this.value = value;
	}

	public DataEntry(Object key, Object value) {
		this.key = key;
		this.value = value;
	}

	public DataEntry(Object key, Object value, int version) {
		this.key = key;
		this.value = value;
		this.version = version;
	}

	public DataEntry(Object key, Object value, int version, int flag) {
		this.key = key;
		this.value = value;
		this.version = version;
		this.flag    = flag;
	}


	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public void decodeMeta(ByteBuffer bytes) {
		magic = bytes.getShort();
		checkSum = bytes.getShort();
		keySize = bytes.getShort();
		version = bytes.getShort();
		padSize = bytes.getInt();
		valueSize = bytes.getInt();
		flag = bytes.get();
		cdate = bytes.getInt();
		mdate = bytes.getInt();
		edate = bytes.getInt();
	}

	public static void encodeMeta(ByteBuffer bytes) {
		bytes.put(DEFAULT_DATA);
	}

	public static void encodeMeta(ByteBuffer bytes, int flag) {
		bytes.put(DEFAULT_DATA);
		if (flag != 0) {
			// setSync flag implicitly
			bytes.put(bytes.position() - 13, (byte)flag);
		}
	}

    public static void encodeMeta(ByteBuffer bytes, DataEntry de) {
        bytes.putShort((short)de.magic);
        bytes.putShort((short)de.checkSum);
        bytes.putShort((short)de.keySize);
        bytes.putShort((short)de.version);
        bytes.putInt(de.padSize);
        bytes.putInt(de.valueSize);
        bytes.put((byte)de.flag);
        bytes.putInt(de.cdate);
        bytes.putInt(de.mdate);
        bytes.putInt(de.edate);
    }

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("key: ").append(key);
		sb.append(", value: ").append(value);
		sb.append(", version: ").append(version).append("\n\t");
		sb.append("cdate: ").append(TairUtil.formatDate(cdate)).append("\n\t");
		sb.append("mdate: ").append(TairUtil.formatDate(mdate)).append("\n\t");
		sb.append("edate: ").append(edate > 0 ? TairUtil.formatDate(edate) : "NEVER").append("\n");
		return sb.toString();
	}

}
