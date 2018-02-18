package com.taobao.tair.etc.compressalg;

import java.io.IOException;
import org.xerial.snappy.Snappy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnappyCompressor implements TairCompressor{
	private static final Logger log = LoggerFactory.getLogger(SnappyCompressor.class);

	public byte[] compress(byte[] in) {
		if (in == null) {
			throw new NullPointerException("Can't compress null");
		}

		byte[] rv = null;

		try {
			rv = Snappy.compress(in);
		} catch (IOException e) {
			throw new RuntimeException("IO exception compressing data(snappy)", e);
		}

		if (log.isDebugEnabled()) {
			log.debug("snappy compressed value, size from [" + in.length + "] to [" + rv.length + "]");
		}

		return rv;
	}

	public byte[] decompress(byte[] in) {
		byte[] rslt = null;
		try {
			rslt = Snappy.uncompress(in);
		} catch (IOException e) {
			rslt = null;
			throw new RuntimeException(e);
		}

		return rslt;
	}
}
