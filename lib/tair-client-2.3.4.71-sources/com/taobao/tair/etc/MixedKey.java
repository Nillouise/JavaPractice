/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.etc;

import java.io.Serializable;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

/*
 * class MixedKey is created for supporting the prefix key function,
 * providing that encoding primary/secondary key into the same ByteBuffer.
 */
public class MixedKey implements Serializable {
    private static final long serialVersionUID = -271828182845904509L;
    private Transcoder transcoder = null;
    private Object pKey = null;
    private Object sKey = null;
    private short prefixSize = 0;
    private byte[] pKeyBytes = null;
    private byte[] sKeyBytes = null;
    private byte[] MixedBytes = null;
    private boolean encoded = false;

    // used in trascoder.decode only
    public MixedKey(Transcoder transcoder) {
        this.transcoder = transcoder;
    }

    // used in localcache only
    public MixedKey(Object pKey, Object sKey) {
        this.pKey = pKey;
        this.sKey = sKey;
    }
    public MixedKey(Transcoder transcoder, Object pKey, Object sKey) {
        this.transcoder = transcoder;
        this.pKey = pKey;
        this.sKey = sKey;
	}

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pKey == null) ? 0 : pKey.hashCode());
		result = prime * result + ((sKey == null) ? 0 : sKey.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MixedKey other = (MixedKey) obj;
		if (pKey == null) {
			if (other.pKey != null)
				return false;
		} else if (!pKey.equals(other.pKey))
			return false;
		if (sKey == null) {
			if (other.sKey != null)
				return false;
		} else if (!sKey.equals(other.sKey))
			return false;
		return true;
	}

    
    /*
     * @COMMENT: encode pkey/skey
     * @RETURN: pKeyBytes if isPrefix, or MixedBytes
     */
    public byte[] encode(boolean isPrefix, boolean withHeader) {
        if (encoded == true) {
            if (isPrefix == true)
                return pKeyBytes;
            return MixedBytes;
        }
        pKeyBytes = transcoder.encode(pKey);
        if (sKey != null){
            sKeyBytes = transcoder.encode(sKey);
            MixedBytes = new byte[pKeyBytes.length + sKeyBytes.length];
        } else {
            MixedBytes = new byte[pKeyBytes.length];
        }

        int len = pKeyBytes.length;
        for (int i = 0; i < len; ++i) {
            MixedBytes[i] = pKeyBytes[i];
        }
        if (sKey != null){
            for (int i = 0; i < sKeyBytes.length; ++i) {
                MixedBytes[len + i] = sKeyBytes[i];
            }
        }

        // if header is true, need to add two extra bytes to the real key
        // the following steps decide the content of pKeyBytes
        // which effect the bucket of this key
        if (withHeader) {
			byte[] tmp = new byte[pKeyBytes.length + 2];
			short flag = TairConstant.TAIR_STYPE_MIXEDKEY;
			flag <<= 1;
			tmp[1] = (byte) (flag & 0xFF);
			tmp[0] = (byte) ((flag >> 8) & 0xFF);
			for (int i = 2; i < tmp.length; ++i) {
				tmp[i] = pKeyBytes[i - 2];
			}
			pKeyBytes = tmp;
        }
        prefixSize = (short)pKeyBytes.length;;

        encoded = true;
        if (isPrefix)
            return pKeyBytes;
        return MixedBytes;
    }

	public MixedKey decode(byte[] bytes, int offset, int size, int prefixSize, boolean withHeader) {
		if (withHeader) {
			prefixSize -= 2;
			pKey = transcoder.decode(bytes, offset, prefixSize);
			sKey = transcoder.decode(bytes, offset + prefixSize, size - prefixSize);
		} else {
			TairUtil.checkMalloc(prefixSize);
			pKey = new byte[prefixSize];
			System.arraycopy(bytes, offset, pKey, 0, prefixSize);
			TairUtil.checkMalloc(size - prefixSize);
			sKey = new byte[size - prefixSize];
			System.arraycopy(bytes, offset + prefixSize, sKey, 0, size - prefixSize);
		}
		return this;
    }

    public short getPrefixSize() {
        return this.prefixSize;
    }

    public Object getPKey() {
        return this.pKey;
    }

    public Object getSKey() {
        return this.sKey;
    }
}
