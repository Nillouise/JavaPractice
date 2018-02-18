/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.DataEntry;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.CounterPack;
import com.taobao.tair.etc.TairUtil;

public class RequestPrefixIncDecPacket extends BasePacket {
    private short namespace = 0;
    private Object pkey = null;
    private int keyCount = 0;
    private List<CounterPack> packList= null;

    public RequestPrefixIncDecPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_PREFIX_INCDEC_PACKET;
    }

    public int encode() {
        Map<MixedKey, CounterPack> keyPackMap = new HashMap<MixedKey, CounterPack>();
        for (CounterPack pack : packList) {
            MixedKey key = new MixedKey(transcoder, pkey, pack.getKey());
            keyPackMap.put(key, pack);
        }
        int size = 40;
        byte[] pkeyBytes;
        try {
            pkeyBytes = transcoder.encode(pkey);
        } catch (Throwable e) {
            return 3;
        }
        size += pkeyBytes.length + 4 + 40;
        Map<byte[], CounterPack> kBytesPackMap = new HashMap<byte[], CounterPack>();
        Map<byte[], MixedKey> kBytesMixedKeyMap = new HashMap<byte[], MixedKey>();
        for (Map.Entry<MixedKey, CounterPack> e : keyPackMap.entrySet()) {
            MixedKey key = e.getKey();
            CounterPack pack = e.getValue();
            byte[] kBytes = null;
            try {
                kBytes = transcoder.encode(key);
            } catch (Throwable nc){
                return 3;
            }
            if (kBytes.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
                return 1;
            }
            size += kBytes.length + 4  + 40 + CounterPack.getEncodedSize();
            kBytesPackMap.put(kBytes, pack);
            kBytesMixedKeyMap.put(kBytes, key);
        }
        writePacketBegin(size);
        byteBuffer.put((byte)0);
        byteBuffer.putShort(namespace);
        fillMetas();
        DataEntry.encodeMeta(byteBuffer);
        byteBuffer.putInt(pkeyBytes.length);
        byteBuffer.put(pkeyBytes);
        //without duplicate key.
        keyCount = kBytesMixedKeyMap.size();
        byteBuffer.putInt(keyCount);
        for (Map.Entry<byte[], MixedKey> e : kBytesMixedKeyMap.entrySet()) {
            MixedKey key = e.getValue();
            byte[] keyBytes = e.getKey();
            int len = keyBytes.length;
            int prefixSize = key.getPrefixSize();
            len |= (prefixSize << 22);

            fillMetas();
            DataEntry.encodeMeta(byteBuffer);
            byteBuffer.putInt(len);
            byteBuffer.put(keyBytes);
            CounterPack pack = kBytesPackMap.get(keyBytes);
            pack.setExpire(TairUtil.getDuration(pack.getExpire()));
            pack.encode(byteBuffer);
        }
        writePacketEnd();

        return 0;
    }

    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    public void setPKey(Object pkey) {
        this.pkey = pkey;
    }

    public void setNamespace(short namespace) {
        this.namespace = namespace;
    }

    public void setPackList(List<CounterPack> packList) {
        this.packList = packList;
        keyCount = packList.size();
    }

    public void addCounter(CounterPack counterPack) {
        if (packList == null) {
            packList = new ArrayList<CounterPack>();
        }
        packList.add(counterPack);
        ++keyCount;
    }
}
