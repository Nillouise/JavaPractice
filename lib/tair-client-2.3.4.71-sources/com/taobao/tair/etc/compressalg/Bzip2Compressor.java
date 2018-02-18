package com.taobao.tair.etc.compressalg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Bzip2Compressor implements TairCompressor{
	private static final Logger log = LoggerFactory.getLogger(Bzip2Compressor.class);

	public byte[] compress(byte[] in) {
		if (in == null) {
			throw new NullPointerException("Can't compress null");
		}

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		BZip2CompressorOutputStream bz2os = null;

		try {
			bz2os = new BZip2CompressorOutputStream(bos);
			bz2os.write(in);
		} catch (IOException e) {
			throw new RuntimeException("IO exception compressing data", e);
		} finally {
			try {
				bz2os.close();
				bos.close();
			} catch (Exception e) {
				// should not happen
			}
		}

		byte[] rv = bos.toByteArray();

		if (log.isDebugEnabled()) {
			log.debug("bzip2 compressed value, size from [" + in.length + "] to [" + rv.length + "]");
		}

		return rv;
	}

	public byte[] decompress(byte[] in) {
		ByteArrayOutputStream bos = null;

		if (in != null) {
			ByteArrayInputStream bis = new ByteArrayInputStream(in);

			bos = new ByteArrayOutputStream();

			BZip2CompressorInputStream bz2is = null;

			try {
				bz2is = new BZip2CompressorInputStream(bis);

				byte[] buf = new byte[8192];
				int r = -1;

				while ((r = bz2is.read(buf)) > 0) {
					bos.write(buf, 0, r);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					bz2is.close();
					bis.close();
					bos.close();
				} catch (Exception e) {
					// should not happen
				}
			}
		}

		return (bos == null) ? null : bos.toByteArray();
	}
}
