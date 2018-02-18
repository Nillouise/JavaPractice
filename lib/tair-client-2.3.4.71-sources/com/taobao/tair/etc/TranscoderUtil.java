/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.etc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.taobao.tair.comm.TairObjectInputStream;
import com.taobao.tair.etc.compressalg.Bzip2Compressor;
import com.taobao.tair.etc.compressalg.GzipCompressor;
import com.taobao.tair.etc.compressalg.SnappyCompressor;
import com.taobao.tair.etc.compressalg.TairCompressor;

public class TranscoderUtil {
	private static TairCompressor[] compressors = null;
	
	static {
		compressors = new TairCompressor[TairConstant.TAIR_COMPRESS_TYPE_NUM];
		compressors[0] = new GzipCompressor();
		compressors[1] = new Bzip2Compressor();
		compressors[2] = new SnappyCompressor();
	}

	public static byte[] encodeLong(long number) {
		byte[] rt = new byte[8];

		rt[7] = (byte) (number & 0xFF);
		rt[6] = (byte) ((number >> 8) & 0xFF);
		rt[5] = (byte) ((number >> 16) & 0xFF);
		rt[4] = (byte) ((number >> 24) & 0xFF);
		rt[3] = (byte) ((number >> 32) & 0xFF);
		rt[2] = (byte) ((number >> 40) & 0xFF);
		rt[1] = (byte) ((number >> 48) & 0xFF);
		rt[0] = (byte) ((number >> 56) & 0xFF);
		return rt;
	}

	public static long decodeLong(byte[] data) {
		long rv = 0;

		for (byte i : data) {
			rv = (rv << 8) | ((i < 0) ? (256 + i)
					: i);
		}

		return rv;
	}

	public static byte[] encodeInt(int number) {
		byte[] fg = new byte[4];

		fg[3] = (byte) (number & 0xFF);
		fg[2] = (byte) ((number >> 8) & 0xFF);
		fg[1] = (byte) ((number >> 16) & 0xFF);
		fg[0] = (byte) ((number >> 24) & 0xFF);
		return fg;
	}

	public static int decodeInt(byte[] data) {
		assert data.length <= 4 : "Too long to be an int (" + data.length + ") bytes";
		return (int) decodeLong(data);
	}

	public static int getInt(byte[] data, int offset) {
		int rv = 0;
		rv = ((data[offset + 3] < 0) ? (256 + data[offset + 3]) : data[offset + 3]);
		rv = (rv << 8) | ((data[offset + 2] < 0) ? (256 + data[offset + 2]) : data[offset + 2]);
		rv = (rv << 8) | ((data[offset + 1] < 0) ? (256 + data[offset + 1]) : data[offset + 1]);
		rv = (rv << 8) | ((data[offset] < 0) ? (256 + data[offset]) : data[offset]);
		return rv;
	}

	public static byte[] encodeByte(byte in) {
		return new byte[] { in };
	}

	public static byte decodeByte(byte[] in) {
		assert in.length <= 1 : "Too long for a byte";

		byte rv = 0;

		if (in.length == 1) {
			rv = in[0];
		}

		return rv;
	}

	public static byte[] encodeBoolean(boolean b) {
		byte[] rv = new byte[1];

		rv[0] = (byte) (b ? '1'
				: '0');
		return rv;
	}

	public static boolean decodeBoolean(byte[] in) {
		assert in.length == 1 : "Wrong length for a boolean";
		return in[0] == '1';
	}

	public static byte[] compress(byte[] in, int compressionType) {
		byte[] rslt = null;

		if ((compressionType >= 0) && (compressionType < compressors.length)
				&& (compressors[compressionType] != null)) {
			rslt = compressors[compressionType].compress(in);
		} else {
			throw new RuntimeException("unknown compress flag: "
					+ compressionType);
		}

		return rslt;
	}

	public static byte[] decompress(byte[] in, int compressionType) {
		byte[] rslt = null;

		if ((compressionType >= 0) && (compressionType < compressors.length)
				&& (compressors[compressionType] != null)) {
			rslt = compressors[compressionType].decompress(in);
		} else {
			throw new RuntimeException("unknown decompress flag: "
					+ compressionType);
		}

		return rslt;
	}

	public static byte[] serialize(Object o) {
		if (o == null) {
			throw new NullPointerException("Can't serialize null");
		}

		byte[] rv = null;

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);

			os.writeObject(o);
			os.close();
			bos.close();
			rv = bos.toByteArray();
		} catch (IOException e) {
			throw new IllegalArgumentException("Non-serializable object", e);
		}

		return rv;
	}

	public static Object deserialize(byte[] in, ClassLoader classLoader) {
		return deserialize(in, classLoader, false);
	}

	public static Object deserialize(byte[] in, ClassLoader classLoader, boolean monitorMode) {
		Object rv = null;

		try {
			if (in != null) {
				ByteArrayInputStream bis = new ByteArrayInputStream(in);
				ObjectInputStream is = new TairObjectInputStream(bis, classLoader);

				rv = is.readObject();
				is.close();
				bis.close();
			}
		} catch (Exception e) {
			if (monitorMode) {
				rv = new String("get the result, but the monitor can't decode your class");
			} else {
				throw new RuntimeException("deserialize failed", e);
			}
		}

		return rv;
	}

	public static String decodeString(byte[] data, String charset) {
		String rv = null;

		try {
			if (data != null) {
				rv = new String(data, charset);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return rv;
	}

	/**
	 * Encode a string into the current character set.
	 */
	public static byte[] encodeString(String in, String charset) {
		byte[] rv = null;

		try {
			rv = in.getBytes(charset);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return rv;
	}

}
