/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.xerial.snappy.Snappy;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.KeyValuePack;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestMPutPacket extends BasePacket {
	private byte serverFlag;
	private short namespace;
	private List<KeyValuePack> records;
	// for uncompress tmp buffer
	private ByteBuffer buffer = null;
	// for compress
	private byte[] packetData = null;
	// for server dup
	private int packetId;

  private static final Logger log = LoggerFactory.getLogger(RequestMPutPacket.class);

	public RequestMPutPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_MPUT_PACKET;
	}

	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}

	public short getNamespace() {
		return namespace;
	}

	public void setRecords(List<KeyValuePack> records) {
		this.records = records;
	}

	public List<KeyValuePack> getRecords() {
		return records;
	}

	public int compress() {
		if (packetData != null) {
			return 0;
		}

		int rc = doEncode(false);
		if (rc == 0) {
			try {
				packetData = Snappy.compress(this.buffer.array());
			} catch (IOException e) {
				return 3;
			}
		}
		return rc;
	}

	public int encode() {
		if (packetData != null) {
			writePacketBegin(packetData.length + TairConstant.INT_SIZE + 1);
			byteBuffer.put((byte)1); // compressed flag
			byteBuffer.putInt(packetData.length);
			byteBuffer.put(packetData);
			writePacketEnd();
		} else {
			return doEncode(true);
		}
		return 0;
	}

  private void printHexString( byte[] b) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < b.length; i++) {
      String hex = Integer.toHexString(b[i] & 0xFF);
      if (hex.length() == 1) {
        hex = '0' + hex;
      }
      sb.append(hex.toUpperCase()+" ");
    }
    log.warn(sb.toString());
  }

	private int doEncode(boolean encodePacket) {
		int length = 0;
		short prefixSize = 0;

		List<byte[]> recordDatas = new ArrayList<byte[]>();
		ArrayList<Integer> recordLength = new ArrayList<Integer>();

		byte[] data = null;
		for (KeyValuePack record : records) {
			// key
			try {
				data = transcoder.encode(record.getKey());
			} catch (Exception e) {
        log.error("key encode error");
				return 3;
			}
			if (data.length > TairConstant.TAIR_KEY_MAX_LENTH) {
				return 1;
			}
			length += data.length + 40; // keyLength + DataEntryMeta
			recordDatas.add(data);
      //printHexString(data);
			// value
		  if (record.getKey() instanceof MixedKey) {
		  	MixedKey mixedKey = (MixedKey)record.getKey();
		  	prefixSize = mixedKey.getPrefixSize();
        mixedKey = null;
		  }
		  int keyLen = prefixSize;
		  keyLen <<= 22;
		  keyLen |= data.length;
      recordLength.add(keyLen);

			try {
				data = transcoder.encode(record.getValue());
			} catch (Exception e) {
        log.error("value encode error");
				return 3;
			} if (data.length > TairConstant.TAIR_VALUE_MAX_LENGTH) {
				return 2;
			}
			length += data.length + 40; // valueLength + DataEntryMeta
			recordDatas.add(data);
			// value entry meta(expire + version)
			length += TairConstant.INT_SIZE + TairConstant.SHORT_SIZE;
		}

		// serverFlag + namespace + records.size() + packetId
		length += 1 + TairConstant.SHORT_SIZE + TairConstant.INT_SIZE + TairConstant.INT_SIZE;

		if (encodePacket) {		// direct encode packet
			writePacketBegin(length + 1); // + compressFlag
			buffer = this.byteBuffer;
			buffer.put((byte)0); // not compress
		} else {
			buffer = ByteBuffer.allocate(length); // no compress flag
		}

		buffer.put((byte)serverFlag);
		buffer.putShort(namespace);
		buffer.putInt(records.size());

		for (int i = 0; i < recordDatas.size(); i += 2) {
			// key
			fillMetas(buffer);
			DataEntry.encodeMeta(buffer);
			buffer.putInt(recordLength.get(i/2));
			buffer.put(recordDatas.get(i));
			// value
			fillMetas(buffer);
			DataEntry.encodeMeta(buffer);
			buffer.putInt(recordDatas.get(i+1).length);
			buffer.put(recordDatas.get(i+1));
			// value entry meta
			buffer.putShort(records.get(i/2).getVersion());
			buffer.putInt(records.get(i/2).getExpire());
		}

		// packtId
		buffer.putInt(packetId);

		if (encodePacket) {
			writePacketEnd();
		}
        return 0;
	}

}

