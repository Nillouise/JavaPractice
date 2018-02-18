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
import java.util.HashMap;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.IncData;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.KeyValuePack;
import com.taobao.tair.etc.TairUtil;

public class RequestPrefixPutsPacket extends BasePacket {
    private short namespace;
    private Object pKey = null;
    private List<KeyValuePack> keyValuePackList= null;

    public RequestPrefixPutsPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_PREFIX_PUTS_PACKET;
    }

    public int encode() {
        HashMap<MixedKey, KeyValuePack> keyValuePackMap = new HashMap<MixedKey, KeyValuePack>();
        for (KeyValuePack kv : keyValuePackList) {
            MixedKey mixedKey = new MixedKey(transcoder, pKey, kv.getKey());
            keyValuePackMap.put(mixedKey, kv);
        }

        HashMap<byte[], Integer> valueFlags = new HashMap<byte[], Integer>();
        HashMap<byte[], byte[]> kvBytes = new HashMap<byte[], byte[]>();
        HashMap<byte[], KeyValuePack> prefixKeySizeMap = new HashMap<byte[], KeyValuePack>();

        int size = 0;
        //~ serialize pkey
        byte[] pKeyByte = null;
        try {
            pKeyByte = transcoder.encode(pKey);
        } catch (Exception e) {
            return 3;
        }
        size += pKeyByte.length;
        size += 40; //~ meta + length
        //~ serialize key/value
        for (MixedKey key : keyValuePackMap.keySet()) {
            KeyValuePack kvPack = keyValuePackMap.get(key);

            byte[] kBytes = null;
            byte[] vBytes = null;
            try {
                kBytes = transcoder.encode(key);
                vBytes = transcoder.encode(kvPack.getValue());
            } catch (Exception e) {
                return 3;
            }
            if (kBytes.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
                return 1;
            }
            if (vBytes.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
                return 2;
            }
            if (kvPack.getValue() instanceof IncData) {
            	valueFlags.put(kBytes, DataEntry.TAIR_ITEM_FLAG_ADDCOUNT);
            }
            else {
            	valueFlags.put(kBytes, 0);
            }
            kvBytes.put(kBytes, vBytes);
            kvPack.setPrefixSize(key.getPrefixSize());
            prefixKeySizeMap.put(kBytes, kvPack);
            size += kBytes.length + vBytes.length;
            size += 40 * 2; //~ meta + length for each key&value
        }
        writePacketBegin(size);
        byteBuffer.put((byte)0);
        byteBuffer.putShort(namespace);
        //~ encode pkey
        fillMetas();
        DataEntry.encodeMeta(byteBuffer);
        byteBuffer.putInt(pKeyByte.length);
        byteBuffer.put(pKeyByte);
        //~ encode key/value
        byteBuffer.putInt(kvBytes.size());
        DataEntry de = new DataEntry();

        for (byte[] key : kvBytes.keySet()) {
            KeyValuePack kvPack = prefixKeySizeMap.get(key);
            de.setVersion(kvPack.getVersion());
            de.setExpiredDate(TairUtil.getDuration(kvPack.getExpire()));
            //~ key
            fillMetas();
            DataEntry.encodeMeta(byteBuffer, de);
            int prefixSize = prefixKeySizeMap.get(key).getPrefixSize();
            int len = ((prefixSize << 22) | key.length);
            byteBuffer.putInt(len);
            byteBuffer.put(key);
            //~ value
            byte[] value = kvBytes.get(key);
            fillMetas();
            int valueFlag = valueFlags.get(key);
            DataEntry.encodeMeta(byteBuffer, valueFlag);
            byteBuffer.putInt(value.length);
            byteBuffer.put(value);
        }

        writePacketEnd();
        return 0;
    }

    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    public void setPKey(Object pKey) {
        this.pKey = pKey;
    }


    public void setNamespace(short namespace) {
        this.namespace = namespace;
    }

    public void setKeyValuePackList(List<KeyValuePack> keyValuePackList) {
        this.keyValuePackList = keyValuePackList;
    }

    public List<KeyValuePack> getKeyValuePackList() {
        return this.keyValuePackList;
    }
}
