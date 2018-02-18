/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import org.xerial.snappy.Snappy;
import java.io.IOException;
import java.nio.ByteBuffer;


public class RequestBulkWriteV2Packet extends BasePacket {
    int token;
    int namespace;
    int bucket;
    int keyCount;
    int size;
    byte[] buf;
    int protocolVerion;
    boolean compress;

    static int BULKWRITE_VERSION_COMPRESS = 3;

    public RequestBulkWriteV2Packet(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_REQ_BULK_WRITE_V2_PACKET;
    }

    public void setCompression(boolean compress)
    {
        this.protocolVerion = BULKWRITE_VERSION_COMPRESS;
        this.compress = compress;
    }

    /**
     * encode
     */
    public int encode() {
        int capacity = 0;
        capacity = 40 + size;
        writePacketBegin(capacity);
        byteBuffer.putInt(protocolVerion);
        if (protocolVerion == BULKWRITE_VERSION_COMPRESS)
        {
          byteBuffer.putInt(token);
          byteBuffer.putInt(namespace);
          byteBuffer.putInt(bucket);
          byteBuffer.putInt(keyCount);
          byteBuffer.putInt(compress ? 1 : 0);
          if (compress)
          {
	          byte[] compressData = null;
            long start = 0, end = 0;
			      try {
              start = System.currentTimeMillis();
			      	compressData = Snappy.rawCompress(buf, size);
              end = System.currentTimeMillis();
			      } catch (Exception e) {
			      	return 3;
			      }
                  /*
            System.out.println("compress size " + size + " " + buf.length + " to " + compressData.length +
                               " comsume " + (end - start) + "ms");
			      byteBuffer.putInt(compressData.length);
			      byteBuffer.put(compressData, 0, compressData.length);
                  */
          }
          else
          {
            byteBuffer.putInt(size);
            byteBuffer.put(buf, 0, size);
          }
        }
        writePacketEnd();
        return 0;
    }

    /**
     * decode
     */
    public boolean decode() {
        throw new UnsupportedOperationException();
    }

    public void setKeyCount(int count) {
        this.keyCount = count;
    }

    public void setToken(int token) {
        this.token = token;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public void setNamespace(int namespace) {
        this.namespace = namespace;
    }

    public void setFileContent(byte[] buf) {
        this.buf = buf;
    }

    public void setFileSize(int size) {
        this.size = size;
    }
}
