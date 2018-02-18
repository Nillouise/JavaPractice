package com.taobao.tair.etc.compressalg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GzipCompressor implements TairCompressor{
	private static final Logger log = LoggerFactory.getLogger(GzipCompressor.class);

	public byte[] compress(byte[] in) {
		if (in == null) {
			throw new NullPointerException("Can't compress null");
		}

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		GZIPOutputStream gz = null;

		try {
			gz = new GZIPOutputStream(bos);
			gz.write(in);
		} catch (IOException e) {
			throw new RuntimeException("IO exception compressing data(gzip)", e);
		} finally {
			try {
				gz.close();
				bos.close();
			} catch (Exception e) {
				// should not happen
			}
		}

		byte[] rv = bos.toByteArray();

		if (log.isInfoEnabled()) {
			log.info("gzip compressed value, size from [" + in.length + "] to [" + rv.length + "]");
		}

		return rv;
	}

	public byte[] decompress(byte[] in) {
		ByteArrayOutputStream bos = null;

		if (in != null) {
			ByteArrayInputStream bis = new ByteArrayInputStream(in);

			bos = new ByteArrayOutputStream();

			GZIPInputStream gis = null;

			try {
				gis = new GZIPInputStream(bis);

				byte[] buf = new byte[8192];
				int r = -1;

				while ((r = gis.read(buf)) > 0) {
					bos.write(buf, 0, r);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					gis.close();
					bis.close();
					bos.close();
				} catch (Exception e) {
					// should not happen
				}
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("gzip compressed value, size from [" + in.length + "] to [" + bos.toByteArray().length + "]");
		}

		return (bos == null) ? null : bos.toByteArray();
	}
}
