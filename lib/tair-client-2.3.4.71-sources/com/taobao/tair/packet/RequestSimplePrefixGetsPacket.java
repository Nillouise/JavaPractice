/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;

public class RequestSimplePrefixGetsPacket extends BasePacket {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RequestPrefixPutsPacket.class);
	
    protected short namespace;
    protected long	reserved;
    
    protected Map<Serializable, List<? extends Serializable>> keyset = new HashMap<Serializable, List<? extends Serializable>>();
  
    public RequestSimplePrefixGetsPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_SIMPLE_GET_PACKET;
    }

    @Override
    public int encode() {
    	int capacity = 8 + 2 +2;
    	Map<byte[], List<byte[]>> keyBytes = new HashMap<byte[], List<byte[]>>(); 
    	for (Entry<? extends Serializable, List<? extends Serializable>> entry : keyset.entrySet()) {
    		try {
				byte[] keybuf = transcoder.encode(entry.getKey());
				reserved = TairUtil.murMurHash(keybuf);
				if (keybuf.length >= 8 * 1024)
					return TairConstant.KEYTOLARGE;
				capacity += 2 + keybuf.length;
				if (entry.getValue().size() != 0) {
					capacity += 2;
				}
				List<byte[]> subkeyBytes = new ArrayList<byte[]>();
				capacity += 2;
				// subkey count
				for (Object subkey : entry.getValue()) {
					// subkey
					byte[] subkeybuf = transcoder.encode(subkey);
					if (subkeybuf.length >= 8 * 1024)
						return TairConstant.KEYTOLARGE;
					capacity += 2 + subkeybuf.length;
					subkeyBytes.add(subkeybuf);
				}
				keyBytes.put(keybuf, subkeyBytes);
    		} catch (Exception e) {
    			LOGGER.warn("encode error ", e);
    			return TairConstant.SERIALIZEERROR;
    		}
    	}
    	writePacketBegin(capacity);
    	byteBuffer.putLong(reserved);
    	byteBuffer.putShort(namespace);
    	// keycount
    	byteBuffer.putShort((short)keyset.size());
    	
    	for (Entry<byte[], List<byte[]>> entry : keyBytes.entrySet()) {
    		try {
				if (entry.getValue().size() == 0) {
					byteBuffer.putShort((short) (entry.getKey().length));
				} else {
					byteBuffer.putShort((short) (entry.getKey().length + 2));
					short flag = TairConstant.TAIR_STYPE_MIXEDKEY;
					flag <<= 1;
					byteBuffer.put((byte) ((flag >> 8) & 0xFF));
					byteBuffer.put((byte) (flag & 0xFF));
				}
				byteBuffer.put(entry.getKey());
				// subkey count
				byteBuffer.putShort((short) (entry.getValue().size()));
				for (byte[] subkey : entry.getValue()) {
					// subkey
					byteBuffer.putShort((short) subkey.length);
					byteBuffer.put(subkey);
				}
    		} catch (Exception e) {
    			LOGGER.warn("encode error ", e);
    			return TairConstant.SERIALIZEERROR;
    		}
    	}
    	writePacketEnd();
        return TairConstant.REQUEST_ENCODE_OK;
    }

    @Override
    public boolean decode() {
        throw new UnsupportedOperationException();
    }
    
    public void setNamespace(short namespace) {
        this.namespace = namespace;
    }
    
    public void setReserved(long reserved) {
    	this.reserved = reserved;
    }
    
    public void addKey(Serializable primeKey, List<? extends Serializable> requestSubKeys) {
    	if (keyset.size() + requestSubKeys.size() >= 1024)
    		throw new IllegalArgumentException("too much keys");
    	keyset.put(primeKey, requestSubKeys);
    }
}
