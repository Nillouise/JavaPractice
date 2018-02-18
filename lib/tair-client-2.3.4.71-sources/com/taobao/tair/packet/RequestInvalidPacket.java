/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;

public class RequestInvalidPacket extends RequestGetPacket {

	protected String groupName;
	protected int isSync;

	public RequestInvalidPacket(Transcoder transcoder, String groupName) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_INVALID_PACKET;
		this.groupName = groupName;
		this.isSync = 0; // CallMode.SYNC
	}

	@Override
	public int encode() {
        int capacity = 0;
        List<byte[]> list = new ArrayList<byte[]>();
        List<MixedKey> prefixSizeList = null;

        for (Object key : keyList) {
            byte[] keyByte;
            try {
                keyByte = transcoder.encode(key);
            } catch (Exception e) {
                return 3;
            }

            if (keyByte.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
                return 1;
            }

            if (key instanceof MixedKey) {
                if (prefixSizeList == null) {
                    prefixSizeList = new ArrayList<MixedKey>();
                }
                MixedKey mixedKey = (MixedKey)key;
                prefixSizeList.add(mixedKey);
            }

            list.add(keyByte);
            capacity += 40;
            capacity += keyByte.length;
        }

		writePacketBegin(capacity);

		// body
		byteBuffer.put((byte) 0);
		byteBuffer.putShort(namespace);
		byteBuffer.putInt(list.size());

        int index = 0;
		for (byte[] keyByte : list) {
            fillMetas();
            DataEntry.encodeMeta(byteBuffer); // 29
            short prefixSize = 0;
            int len = 0;
            if (prefixSizeList != null) {
                MixedKey mixedKey = prefixSizeList.get(index);
                prefixSize = mixedKey.getPrefixSize();
                len = prefixSize;
                len <<= 22;
                len |= keyByte.length;
            } else {
                len = keyByte.length;
            }
            byteBuffer.putInt(len);// 4
            byteBuffer.put(keyByte); // 33+length
            ++index;
		}

		writeString(getGroupName());
		byteBuffer.putInt(isSync);

		writePacketEnd();

		return 0;
	}

	@Override
	public boolean decode() {
        throw new UnsupportedOperationException();
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public void setSync(int is_sync) {
		this.isSync = is_sync;
	}

	public int getSync() {
		return this.isSync;
	}
}
