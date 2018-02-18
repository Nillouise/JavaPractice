/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */

package com.taobao.tair.impl;
import java.util.HashMap;
import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.ResultCode;

public class StatisticsResult {
  private Map<String, Map<String, Long>> areaStat = new HashMap<String, Map<String, Long>>();
  private Map<String, Map<String, Long>> dataserverStat = new HashMap<String, Map<String, Long>>();
  protected boolean addOneDsAreaStat(String dataserver, byte[] statData, StatisticsAnalyser statAnalyser) {
    long[] stat = statAnalyser.analyseStatisticsByte(statData);
    String[] itemName = statAnalyser.getItemName();
    if (stat == null || itemName == null)
      return false;
    Map<String, Long> oneDsStat = dataserverStat.get(dataserver);
    if (oneDsStat == null) {
      oneDsStat = new HashMap<String, Long>();
      dataserverStat.put(dataserver, oneDsStat);
    }
    for (int oneAreaStatStart = 0; oneAreaStatStart < stat.length; oneAreaStatStart += itemName.length) {
      String area = new String(Long.toString(stat[oneAreaStatStart]));
      for (int i = 1; i < itemName.length; ++i) {
        long statNumerical = stat[oneAreaStatStart + i];
        // update dataserverStat
        Long itemStat = oneDsStat.get(itemName[i]);
        if (itemStat != null)
          oneDsStat.put(itemName[i], new Long(itemStat.longValue() + statNumerical));
        else
          oneDsStat.put(itemName[i], new Long(statNumerical));

        // update Area Stat
        Map<String, Long> oneAreaStat = areaStat.get(area);
        if (oneAreaStat == null) {
          oneAreaStat = new HashMap<String, Long>();
          areaStat.put(area, oneAreaStat);
        }
        Long oneItemStat = oneAreaStat.get(itemName[i]);
        if (oneItemStat != null)
          oneAreaStat.put(itemName[i], new Long(oneItemStat.longValue() + statNumerical));
        else
          oneAreaStat.put(itemName[i], new Long(statNumerical));
      }
    }// end of for
    return true;
  }

  public void clear() {
    areaStat = new HashMap<String, Map<String, Long>>();
    dataserverStat = new HashMap<String, Map<String, Long>>();
  }

  /**
   * @return the areaStat
   */
  public Map<String, Map<String, Long>> getAreaStat()
  {
    return areaStat;
  }

  /**
   * @return the dataserverStat
   */
  public Map<String, Map<String, Long>> getDataserverStat()
  {
    return dataserverStat;
  }
}

class StatisticsAnalyser {
  private static final Logger log = LoggerFactory.getLogger(StatisticsAnalyser.class);
  // sizeof byte of each stat item
  private int[] itemSizeof;
  private String[] itemName;
  private int itemCount;
  private int oneRowSizeByteBeforeAnalyse;
  private int version;
  private DecodeInteger decodeInteger;

  boolean init(byte[] schemaByte, int schemaVersion) {
    if (schemaByte.length < 5)
      return false;
    int pos = 0;
    boolean ret = true;

    try {
      Charset charset = Charset.forName("UTF-8");
      // getByteOrder, '0' little endian, '1', big endian
      byte packet_byteorder = schemaByte[pos++];
      if (packet_byteorder == (byte)'0')
        decodeInteger = new DecodeLittleEndianInteger();
      else
        decodeInteger = new DecodeBigEndianInteger();

      // get item count
      itemCount = (int)decodeInteger.decode(schemaByte, pos, 4);
      pos += 4;

      // get item name, [item0_name_len][item0_name_str][item1_name_len][item1_name_str]...
      itemName = new String[itemCount];
      for (int i = 0; i < itemCount; ++i) {
        int itemNameLen = (int)decodeInteger.decode(schemaByte, pos, 4);
        pos += 4;
        itemName[i] = new String(schemaByte, pos, itemNameLen, charset);
        pos += itemNameLen;
      }

      oneRowSizeByteBeforeAnalyse = 0;
      itemSizeof = new int[itemCount];
      for (int i = 0; i < itemCount; ++i) {
        int itemSizeofLen = (int)decodeInteger.decode(schemaByte, pos, 4);
        pos += 4;
        itemSizeof[i] = itemSizeofLen;
        oneRowSizeByteBeforeAnalyse += itemSizeofLen;
      }

      version = schemaVersion;
    }
    catch (IllegalCharsetNameException e)
    {
      ret = false;
    }
    catch (IllegalArgumentException e)
    {
      ret = false;
    }
    catch (IndexOutOfBoundsException e)
    {
      ret = false;
    }

    return ret;
  }

  void clear() {
    itemSizeof = null;
    itemName = null;
    itemCount = 0;
    oneRowSizeByteBeforeAnalyse = 0;
    version = 0;
    decodeInteger = null;
  }

  long[] analyseStatisticsByte(byte[] statisticsByte) {
    if (statisticsByte.length == 0)
      return null;

    // simple check
    if (statisticsByte.length % oneRowSizeByteBeforeAnalyse != 0) {
      log.error("parsing stat, stat length dismatch");
      return null;
    }

    int pos = 0;
    long[] result = new long[(statisticsByte.length / oneRowSizeByteBeforeAnalyse) * itemCount];
    int resultIndex = 0;
    while(pos < statisticsByte.length) {
      for (int i = 0; i < itemCount; ++i) {
        long data = decodeInteger.decode(statisticsByte, pos, itemSizeof[i]);
        pos += itemSizeof[i];
        result[resultIndex++] = data;
      }
    }
    return result;
  }

  public int getItemCount() {
    return itemCount;
  }

  public String[] getItemName() {
    return itemName;
  }

  public int getSchemaVersion() {
    return version;
  }
}

interface DecodeInteger {
  long decode(byte[] data, int start, int integerByteCount);
}

class DecodeLittleEndianInteger implements DecodeInteger {
  public long decode(byte[] data, int start, int integerByteCount) {
    switch (integerByteCount)
    {
    case 1:
      return (long)data[start] & 0xff;
    case 2:
      return
        (((long)data[start + 1] & 0xff) << 8)
        | ((long)data[start] & 0xff);
    case 4:
      return
        (((long)data[start + 3] & 0xff) << 24)
        | (((long)data[start + 2] & 0xff) << 16)
        | (((long)data[start + 1] & 0xff) << 8)
        | ((long)data[start] & 0xff);
    case 8:
      return (((long)data[start + 7] & 0xff) << 56)
        | (((long)data[start + 6] & 0xff) << 48)
        | (((long)data[start + 5] & 0xff) << 40)
        | (((long)data[start + 4] & 0xff) << 32)
        | (((long)data[start + 3] & 0xff) << 24)
        | (((long)data[start + 2] & 0xff) << 16)
        | (((long)data[start + 1] & 0xff) << 8)
        | ((long)data[start] & 0xff);
    }
    return 0;
  }
}

class DecodeBigEndianInteger implements DecodeInteger {
  public long decode(byte[] data, int start, int integerByteCount) {
    switch (integerByteCount)
    {
    case 1:
      return (long)data[start] & 0xff;
    case 2:
      return
        (((long)data[start] & 0xff) << 8)
        | ((long)data[start + 1] & 0xff);
    case 4:
      return
        (((long)data[start] & 0xff) << 24)
        | (((long)data[start + 1] & 0xff) << 16)
        | (((long)data[start + 2] & 0xff) << 8)
        | ((long)data[start + 3] & 0xff);
    case 8:
      return (((long)data[start] & 0xff) << 56)
        | (((long)data[start + 1] & 0xff) << 48)
        | (((long)data[start + 2] & 0xff) << 40)
        | (((long)data[start + 3] & 0xff) << 32)
        | (((long)data[start + 4] & 0xff) << 24)
        | (((long)data[start + 5] & 0xff) << 16)
        | (((long)data[start + 6] & 0xff) << 8)
        | ((long)data[start + 7] & 0xff);
    }
    return 0;
  }
}
