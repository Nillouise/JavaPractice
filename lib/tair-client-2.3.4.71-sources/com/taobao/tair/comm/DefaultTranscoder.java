/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import java.util.Date;

import com.taobao.tair.etc.IncData;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairIllegalArgumentException;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.etc.TranscoderUtil;

/**
 * the default transcoder impl
 */
public class DefaultTranscoder implements Transcoder {
	private int compressionThreshold = TairConstant.TAIR_DEFAULT_COMPRESSION_THRESHOLD;
	private int compressionType = TairConstant.TAIR_DEFAULT_COMPRESSION_TYPE;
	private String charset = TairConstant.DEFAULT_CHARSET;

	private boolean withHeader = true;
	private boolean monitorMode = false;
	private static final int COMPRESSIONTYPEOFFSET = 12;

	private ClassLoader customClassLoader = DefaultTranscoder.class.getClassLoader();

	public void setWithHeader(boolean f) {
		this.withHeader = f;
	}

	public void setMonitorMode(boolean f) {
		this.monitorMode = f;
	}

	public DefaultTranscoder(int compressionThreshold, String charset) {
		if (compressionThreshold > 0) {
			this.compressionThreshold = compressionThreshold;
		}

		if (charset != null) {
			this.charset = charset;
		}
	}

    public DefaultTranscoder(int compressionThreshold, String charset, int compressionType) {
        if (compressionThreshold > 0) {
            this.compressionThreshold = compressionThreshold;
        }

        if (charset != null) {
            this.charset = charset;
        }

        if ((compressionType >= 0) && (compressionType < TairConstant.TAIR_COMPRESS_TYPE_NUM)) {
			this.compressionType = compressionType;
        }
    }

	public String getCharset() {
		return charset;
	}

    public byte[] encode(Object object) {
        return encode(object, false);
    }
    
    public void setCustomClassLoader(ClassLoader customClassLoader) {
    	this.customClassLoader = customClassLoader;
    }

    public byte[] encode(Object object, boolean prefixCared) {
        if (object == null) {
            throw new TairIllegalArgumentException("key,value can not be null", TairConstant.KEYORVALUEISNULL);
        }

        if (object instanceof MixedKey && prefixCared) {
            MixedKey mixedKey = (MixedKey) object;
            return mixedKey.encode(true, withHeader); //~ only return encoded prefix
        }

        byte[] b    = null;
        short  flag = 0;
        boolean toCompress = true;

        if (object instanceof String) {
            b    = TranscoderUtil.encodeString((String) object, charset);
            flag = TairConstant.TAIR_STYPE_STRING;
        } else if (object instanceof Long) {
            b    = TranscoderUtil.encodeLong((Long) object);
            flag = TairConstant.TAIR_STYPE_LONG;
        } else if (object instanceof Integer) {
            b    = TranscoderUtil.encodeInt((Integer) object);
            flag = TairConstant.TAIR_STYPE_INT;
        } else if (object instanceof Boolean) {
            b    = TranscoderUtil.encodeBoolean((Boolean) object);
            flag = TairConstant.TAIR_STYPE_BOOL;
        } else if (object instanceof Date) {
            b    = TranscoderUtil.encodeLong(((Date) object).getTime());
            flag = TairConstant.TAIR_STYPE_DATE;
        } else if (object instanceof Byte) {
            b    = TranscoderUtil.encodeByte((Byte) object);
            flag = TairConstant.TAIR_STYPE_BYTE;
        } else if (object instanceof Float) {
            b    = TranscoderUtil.encodeInt(Float.floatToRawIntBits((Float) object));
            flag = TairConstant.TAIR_STYPE_FLOAT;
        } else if (object instanceof Double) {
            b    = TranscoderUtil.encodeLong(Double.doubleToRawLongBits((Double) object));
            flag = TairConstant.TAIR_STYPE_DOUBLE;
        } else if (object instanceof byte[]) {
            b    = (byte[]) object;
            flag = TairConstant.TAIR_STYPE_BYTEARRAY;
        } else if (object instanceof IncData) {
            b    = IncData.encode((IncData)object);
            flag = TairConstant.TAIR_STYPE_INCDATA;
        } else if (object instanceof MixedKey) {
            MixedKey mixedKey = (MixedKey) object;
            b = mixedKey.encode(false, withHeader); //~ encode regardless of prefix
            flag = TairConstant.TAIR_STYPE_MIXEDKEY;
            toCompress = false; //~ not to compress mixed key, for the sake of flag consistency
        } else {
            b    = TranscoderUtil.serialize(object);
            flag = TairConstant.TAIR_STYPE_SERIALIZE;
        }

        flag <<= 1;

        if (b.length > compressionThreshold && toCompress) {
			b = TranscoderUtil.compress(b, this.compressionType);
			short type = (short) this.compressionType;
			type <<= DefaultTranscoder.COMPRESSIONTYPEOFFSET;
			flag += type;
			flag += 1;
        }

        TairUtil.checkMalloc(b.length + 2);

        if (false == withHeader) {
            return b;
        }
        byte[] result = new byte[b.length + 2];
        byte[] fg     = new byte[2];

        fg[1]         = (byte) (flag & 0xFF);
        fg[0]         = (byte) ((flag >> 8) & 0xFF);

        for (int i = 0; i < 2; i++) {
            result[i] = fg[i];
        }

        for (int i = 0; i < b.length; i++) {
            result[i + 2] = b[i];
        }

        return result;
    }

    public Object decode(byte[] data) {
        return decode(data, 0, data.length);
    }

    public Object decode(byte[] data, int prefixSize) {
        return decode(data, 0, data.length, prefixSize);
    }

    public Object decode(byte[] data, int offset, int size) {
        return decode(data, offset, size, 0);
    }

    public Object decode(byte[] data, int offset, int size, int prefixSize) {
        ///////////////////////////////////////////////////////////
		if (false == withHeader) {
			// as header is false, only return two object type: bytes or MixedKey
			if (0 == prefixSize) {
				TairUtil.checkMalloc(size);
				byte[] vb = new byte[size];
				System.arraycopy(data, offset, vb, 0, size);
				return vb;
			} else {
				return new MixedKey(this).decode(data, offset, size, prefixSize, withHeader);
			}
		}
    	///////////////////////////////////////////////////////////
        /////////////////////判断并获取是否是一个整形数值  work with withHeader == true///////////
        int index = 0;
        for(index = 0; index < size && (index + offset < data.length); index++) {
            if(data[index+offset] == '+' || data[index+offset] == '-') {
                continue;
            }
            if(data[index+offset] < '0' || data[index+offset] > '9') {
                break;
            }
        }
        if(index == size) {
            String tmpstr = new String(data, offset, size);
            try {
                long tmp = Long.valueOf(tmpstr);
                return tmp;
            } catch(NumberFormatException e) {

            }
        }

        TairUtil.checkMalloc(size - 2);

        byte[] vb = new byte[size - 2];

        System.arraycopy(data, offset + 2, vb, 0, size - 2);

        Object obj = null;

        int    flags = 0;

        for (int i = 0; i < 2; i++) {
            byte b = data[offset + i];

            flags = (flags << 8) | ((b < 0) ? (256 + b)
                    : b);
        }

        if ((flags & 1) == 1) {
			// the high four bits of flag represent the type of compression
			int type = flags >> DefaultTranscoder.COMPRESSIONTYPEOFFSET;
			vb = TranscoderUtil.decompress(vb, type);
        }

        int type = (flags >> 1) & 0xF;

        switch (type) {
            case TairConstant.TAIR_STYPE_INT:
                obj = TranscoderUtil.decodeInt(vb);
                break;

            case TairConstant.TAIR_STYPE_STRING:
                obj = TranscoderUtil.decodeString(vb, charset);
                break;

            case TairConstant.TAIR_STYPE_BOOL:
                obj = TranscoderUtil.decodeBoolean(vb);
                break;

            case TairConstant.TAIR_STYPE_LONG:
                obj = TranscoderUtil.decodeLong(vb);
                break;

            case TairConstant.TAIR_STYPE_DATE:

                Long time = TranscoderUtil.decodeLong(vb);

                obj = new Date(time);
                break;

            case TairConstant.TAIR_STYPE_BYTE:
                obj = TranscoderUtil.decodeByte(vb);
                break;

            case TairConstant.TAIR_STYPE_FLOAT:

                Integer f = TranscoderUtil.decodeInt(vb);

                obj = new Float(Float.intBitsToFloat(f));
                break;

            case TairConstant.TAIR_STYPE_DOUBLE:

                Long l = TranscoderUtil.decodeLong(vb);

                obj = new Double(Double.longBitsToDouble(l));
                break;

            case TairConstant.TAIR_STYPE_BYTEARRAY:
                obj = vb;
                break;

            case TairConstant.TAIR_STYPE_SERIALIZE:
                obj = TranscoderUtil.deserialize(vb, customClassLoader, monitorMode);
                break;

            case TairConstant.TAIR_STYPE_INCDATA:
                obj = IncData.decode(vb);
                break;

            case TairConstant.TAIR_STYPE_MIXEDKEY:
                obj = new MixedKey(this).decode(vb, 0, vb.length, prefixSize, withHeader);
                break;

            default:
                throw new RuntimeException("unknow serialize flag: " + type);
        }

        return obj;
    }

}
