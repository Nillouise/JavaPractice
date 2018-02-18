/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */

package com.taobao.tair.packet;

import java.io.IOException;

import com.taobao.tair.etc.TairConstant;

import org.xerial.snappy.Snappy;

public class ResponseStatisticsPacket extends BasePacket {
  protected int configVersion;
  protected int schemaVersion;
  protected byte[] snappyCompressData;
  protected byte[] schema;

  public ResponseStatisticsPacket() {
    super();
    pcode = TairConstant.TAIR_RESP_STATISTICS_PACKET;
  }

  public int encode() {
    throw new UnsupportedOperationException();
  }

  public boolean decode() {
    configVersion = byteBuffer.getInt();
    int snappyCompressDataLen = byteBuffer.getInt();
    int schemaLen = byteBuffer.getInt();
    schemaVersion = byteBuffer.getInt();

    snappyCompressData = new byte[snappyCompressDataLen];
    byteBuffer.get(snappyCompressData);

    schema = new byte[schemaLen];
    byteBuffer.get(schema);
    return true;
  }

  @Override
  public int getConfigVersion() {
    return configVersion;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public byte[] getDataByte() {
    byte[] data = new byte[0];
    try {
      data = Snappy.uncompress(snappyCompressData);
    }
    catch (IOException e) {
      data = new byte[0];
    }
    return data;
  }

  public byte[] getSchemaByte() {
    return schema;
  }
}
