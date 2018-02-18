/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import com.taobao.tair.etc.TairConstant;

public class RequestStatisticsPacket extends BasePacket {
  protected byte flag;

  public RequestStatisticsPacket() {
    super();
    pcode = TairConstant.TAIR_REQ_STATISTICS_PACKET;
  }

  public int encode() {
    writePacketBegin(1);
    byteBuffer.put(flag);
    writePacketEnd();
    return 0;
  }

  /**
   * decode, not need
   */
  public boolean decode() {
    throw new UnsupportedOperationException();
  }

  public void setRetrieveSchemaFlag(boolean retrieveSchema) {
    if (retrieveSchema)
      flag |= 1;
    else
      flag &= ~1;
  }

  public boolean getRetrieveSchemaFlag() {
    return (flag & 1) == 1;
  }
}
