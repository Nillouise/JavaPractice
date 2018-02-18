/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.etc;

public class TairConstant {
    // packet flag
    public static final int TAIR_PACKET_FLAG = 0x6d426454;

  // Integer/Long constant length
  public static final int SHORT_SIZE = Short.SIZE / 8;
  public static final int INT_SIZE = Integer.SIZE / 8;
  public static final int LONG_SIZE = Long.SIZE / 8;
  //item flag
  public static int TAIR_ITEM_FLAG_ADDCOUNT = 1;
  public static int TAIR_ITEM_FLAG_DELETED = 2;
  public static int TAIR_ITEM_FLAG_ITEM = 4;
  public static int TAIR_ITEM_FLAG_LOCKED = 8;
  public static int TAIR_ITEM_FLAG_NEGLECTED = TAIR_ITEM_FLAG_ITEM;

    // 9xxx flow and stat control code
  public static final int TAIR_STAT_CMD_VIEW = 9000;
  public static final int TAIR_FLOW_CONTROL = 9001;
  public static final int TAIR_FLOW_CONTROL_SET = 9002;
  public static final int TAIR_REQ_FLOW_VIEW = 9003;
  public static final int TAIR_RESP_FLOW_VIEW = 9004;
  public static final int TAIR_FLOW_CHECK = 9005;

    // packet code
    // request
    public static final int TAIR_REQ_PUT_PACKET    = 1;
    public static final int TAIR_REQ_GET_PACKET    = 2;
    public static final int TAIR_REQ_REMOVE_PACKET = 3;
    public static final int TAIR_REQ_REMOVE_AREA   = 4;
    public static final int TAIR_REQ_PING_PACKET = 6;
    public static final int TAIR_REQ_INCDEC_PACKET = 11;
    public static final int TAIR_REQ_LOCK_PACKET   = 14;
    public static final int TAIR_REQ_MPUT_PACKET   = 15;
    public static final int TAIR_REQ_OP_CMD_PACKET   = 16;
    public static final int TAIR_RESP_OP_CMD_PACKET   = 17;
    public static final int TAIR_REQ_GET_RANGE_PACKET= 18;
    public static final int TAIR_RESP_GET_RANGE_PACKET= 19;

    public static final int TAIR_REQ_HIDE_PACKET = 20;
    public static final int TAIR_REQ_HIDE_BY_PROXY_PACKET = 21;
    public static final int TAIR_REQ_GET_HIDDEN_PACKET = 22;
    public static final int TAIR_REQ_INVALID_PACKET = 23;
    public static final int TAIR_REQ_PREFIX_PUTS_PACKET = 24;
    public static final int TAIR_REQ_PREFIX_REMOVES_PACKET = 25;
    public static final int TAIR_REQ_PREFIX_INCDEC_PACKET = 26;
    public static final int TAIR_RESP_PREFIX_INCDEC_PACKET = 27;
    public static final int TAIR_RESP_MRETURN_PACKET = 28;
    public static final int TAIR_REQ_PREFIX_GETS_PACKET = 29;
    public static final int TAIR_RESP_PREFIX_GETS_PACKET = 30;
    public static final int TAIR_REQ_PREFIX_HIDES_PACKET = 31;
    public static final int TAIR_REQ_PREFIX_INVALIDS_PACKET = 32;
    public static final int TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET = 33;
    public static final int TAIR_REQ_PREFIX_GET_HIDDENS_PACKET = 34;

    public static final int TAIR_REQ_SIMPLE_GET_PACKET = 36;
    public static final int TAIR_RESP_SIMPLE_GET_PACKET = 37;
    // response
    public static final int TAIR_RESP_RETURN_PACKET = 101;
    public static final int TAIR_RESP_GET_PACKET    = 102;
    public static final int TAIR_RESP_INCDEC_PACKET    = 105;

    // config server
    public static final int TAIR_REQ_GET_GROUP_NEW_PACKET  = 1002;
    public static final int TAIR_RESP_GET_GROUP_NEW_PACKET = 1102;
    public static final int TAIR_REQ_QUERY_INFO_PACKET = 1009;
    public static final int TAIR_RESP_QUERY_INFO_PACKET = 1106;

    public static final int TAIR_RESP_FEEDBACK_PACKET = 1113;

    // query stat from dataserver
    public static final int TAIR_REQ_STATISTICS_PACKET = 1010;
    public static final int TAIR_RESP_STATISTICS_PACKET =1107;

    // invalidate server
    public static final String INVALID_SERVERLIST_KEY = "invalidate_server";
    // admin server
    public static final String ADMIN_SERVERLIST_KEY = "admin_server_list";

    // items
    public static final int TAIR_REQ_ADDITEMS_PACKET = 1400;
    public static final int TAIR_REQ_GETITEMS_PACKET = 1401;
    public static final int TAIR_REQ_REMOVEITEMS_PACKET = 1402;
    public static final int TAIR_REQ_GETANDREMOVEITEMS_PACKET = 1403;
    public static final int TAIR_REQ_GETITEMSCOUNT_PACKET = 1404;
    public static final int TAIR_RESP_GETITEMS_PACKET = 1405;
    public static final int TAIR_REQ_GET_EXPIRE_PACKET = 1600;
    public static final int TAIR_RESP_GET_EXPIRE_EACKET = 1601;
    public static final int TAIR_REQ_PUT_MODIFY_DATE_PACKET = 1700;
    public static final int TAIR_REQ_GET_MODIFY_DATE_PACKET = 1702;
    public static final int TAIR_RESP_GET_MODIFY_DATE_PACKET = 1703;

    public static final int TAIR_REQ_INC_DEC_BOUNDED_PACKET = 1704;
    public static final int TAIR_RESP_INC_DEC_BOUNDED_PACKET = 1705;

    public static final int TAIR_REQ_PREFIX_INCDEC_BOUNDED_PACKET = 1706;
    public static final int TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET = 1707;


    public enum MCOPS {
      SET((short)0x01, "set"),
      ADD((short)0x02, "add"),
      REPLACE((short)0x03, "replace"),
      INCR((short)0x05, "incr"),
      DECR((short)0x06, "decr"),
      APPEND((short)0x0E, "append"),
      PREPEND((short)0x0F, "prepend"),
      TOUCH((short)0x1C, "touch");

      private final short code;
      private final String name;

      private MCOPS(short code, String name) {
        this.code = code;
        this.name = name;
      }

      public String getName() {
        return name;
      }

      public short getCode() {
        return code;
      }
    }

    public enum EngineType {
      COMMON,
      RDB
    };

    /*
     *  extend for redis 2[12]xx,
     *  first number 2 means extend packet,
     *  second number 1 means request, 2 means response
     */
    //string
    public final static int TAIR_REQ_PUTNX_PACKET = 2149;
    //list
    public final static int TAIR_REQ_LPOP_PACKET = 2100;
    public final static int TAIR_RESP_LPOP_PACKET = 2200;

    public final static int TAIR_REQ_LPUSH_PACKET = 2101;
    public final static int TAIR_RESP_LPUSH_PACKET = 2201;

    public final static int TAIR_REQ_RPOP_PACKET = 2102;
    public final static int TAIR_RESP_RPOP_PACKET = 2202;

    public final static int TAIR_REQ_RPUSH_PACKET = 2103;
    public final static int TAIR_RESP_RPUSH_PACKET = 2203;

    public final static int TAIR_REQ_LPUSHX_PACKET = 2104;
    public final static int TAIR_RESP_LPUSHX_PACKET = 2204;

    public final static int TAIR_REQ_RPUSHX_PACKET = 2105;
    public final static int TAIR_RESP_RPUSHX_PACKET = 2205;

    public final static int TAIR_REQ_LINDEX_PACKET = 2106;
    public final static int TAIR_RESP_LINDEX_PACKET = 2206;

    public final static int TAIR_REQ_LTRIM_PACKET = 2128;
    public final static int TAIR_RESP_LTRIM_PACKET = 2228;

    public final static int TAIR_REQ_LREM_PACKET = 2129;
    public final static int TAIR_RESP_LREM_PACKET = 2229;

    public final static int TAIR_REQ_LRANGE_PACKET = 2130;
    public final static int TAIR_RESP_LRANGE_PACKET = 2230;

    public final static int TAIR_REQ_LLEN_PACKET = 2133;
    public final static int TAIR_RESP_LLEN_PACKET = 2233;

    public final static int TAIR_REQ_LPUSH_LIMIT_PACKET = 2157;
    public final static int TAIR_REQ_RPUSH_LIMIT_PACKET = 2158;
    public final static int TAIR_REQ_LPUSHX_LIMIT_PACKET = 2159;
    public final static int TAIR_REQ_RPUSHX_LIMIT_PACKET = 2160;

    //hset
    public final static int TAIR_REQ_HGETALL_PACKET = 2107;
    public final static int TAIR_RESP_HGETALL_PACKET = 2207;

    public final static int TAIR_REQ_HINCRBY_PACKET = 2108;
    public final static int TAIR_RESP_HINCRBY_PACKET = 2208;

    public final static int TAIR_REQ_HMSET_PACKET = 2109;
    public final static int TAIR_RESP_HMSET_PACKET = 2209;

    public final static int TAIR_REQ_HSET_PACKET = 2110;
    public final static int TAIR_RESP_HSET_PACKET = 2210;

    public final static int TAIR_REQ_HSETNX_PACKET = 2111;
    public final static int TAIR_RESP_HSETNX_PACKET = 2211;

    public final static int TAIR_REQ_HGET_PACKET = 2112;
    public final static int TAIR_RESP_HGET_PACKET = 2212;

    public final static int TAIR_REQ_HMGET_PACKET = 2113;
    public final static int TAIR_RESP_HMGET_PACKET = 2213;

    public final static int TAIR_REQ_HVALS_PACKET = 2114;
    public final static int TAIR_RESP_HVALS_PACKET = 2214;

    public final static int TAIR_REQ_HDEL_PACKET = 2115;
    public final static int TAIR_RESP_HDEL_PACKET = 2215;

    public final static int TAIR_REQ_HLEN_PACKET = 2136;
    public final static int TAIR_RESP_HLEN_PACKET = 2236;

    public final static int TAIR_REQ_HEXISTS_PACKET = 2161;

    //set
    public final static int TAIR_REQ_SCARD_PACKET = 2116;
    public final static int TAIR_RESP_SCARD_PACKET = 2216;

    public final static int TAIR_REQ_SMEMBERS_PACKET = 2117;
    public final static int TAIR_RESP_SMEMBERS_PACKET = 2217;

    public final static int TAIR_REQ_SADD_PACKET = 2118;
    public final static int TAIR_RESP_SADD_PACKET = 2218;

    public final static int TAIR_REQ_SPOP_PACKET = 2119;
    public final static int TAIR_RESP_SPOP_PACKET = 2219;

    public final static int TAIR_REQ_SREM_PACKET = 2145;
    public final static int TAIR_RESP_SREM_PACKET = 2245;

    public final static int TAIR_REQ_SADDMULTI_PACKET = 2146;
    public final static int TAIR_RESP_SADDMULTI_PACKET = 2246;

    public final static int TAIR_REQ_SREMMULTI_PACKET = 2147;
    public final static int TAIR_RESP_SREMMULTI_PACKET = 2247;

    public final static int TAIR_REQ_SMEMBERSMULTI_PACKET = 2148;
    public final static int TAIR_RESP_SMEMBERSMULTI_PACKET = 2248;

    //zset
    public final static int TAIR_REQ_ZRANGE_PACKET = 2120;
    public final static int TAIR_RESP_ZRANGE_PACKET = 2220;

    public final static int TAIR_REQ_ZREVRANGE_PACKET = 2121;
    public final static int TAIR_RESP_ZREVRANGE_PACKET = 2221;

    public final static int TAIR_REQ_ZSCORE_PACKET = 2122;
    public final static int TAIR_RESP_ZSCORE_PACKET = 2222;

    public final static int TAIR_REQ_ZRANGEBYSCORE_PACKET = 2123;
    public final static int TAIR_RESP_ZRANGEBYSCORE_PACKET = 2223;

    public final static int TAIR_REQ_ZADD_PACKET = 2124;
    public final static int TAIR_RESP_ZADD_PACKET = 2224;

    public final static int TAIR_REQ_ZRANK_PACKET = 2125;
    public final static int TAIR_RESP_ZRANK_PACKET = 2225;

    public final static int TAIR_REQ_ZCARD_PACKET = 2126;
    public final static int TAIR_RESP_ZCARD_PACKET = 2226;

    public final static int TAIR_REQ_ZREM_PACKET = 2137;
    public final static int TAIR_RESP_ZREM_PACKET = 2237;

    public final static int TAIR_REQ_ZREMRANGEBYRANK_PACKET = 2138;
    public final static int TAIR_RESP_ZREMRANGEBYRANK_PACKET = 2238;

    public final static int TAIR_REQ_ZREMRANGEBYSCORE_PACKET = 2139;
    public final static int TAIR_RESP_ZREMRANGEBYSCORE_PACKET = 2239;

    public final static int TAIR_REQ_ZREVRANK_PACKET = 2140;
    public final static int TAIR_RESP_ZREVRANK_PACKET = 2240;

    public final static int TAIR_REQ_ZCOUNT_PACKET = 2141;
    public final static int TAIR_RESP_ZCOUNT_PACKET = 2241;

    public final static int TAIR_REQ_ZINCRBY_PACKET = 2142;
    public final static int TAIR_RESP_ZINCRBY_PACKET = 2242;

    public final static int TAIR_RESP_ZRANGEWITHSCORE_PACKET = 2243;
    public final static int TAIR_RESP_ZREVRANGEWITHSCORE_PACKET = 2244;

    public final static int TAIR_REQ_GENERIC_ZRANGEBYSCORE_PACKET = 2151;
    public final static int TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET = 2251;

    public final static int TAIR_REQ_LAZY_REMOVE_AREA_PACKET = 2152;

    public final static int TAIR_REQ_EXISTS_PACKET = 2153;

    public final static int TAIR_REQ_INFO_PACKET = 2154;
    public final static int TAIR_RESP_INFO_PACKET = 2254;

    public final static int TAIR_REQ_GETSET_PACKET = 2155;
    public final static int TAIR_RESP_GETSET_PACKET = 2255;

    //common
    public final static int TAIR_REQ_EXPIRE_PACKET = 2127;
    public final static int TAIR_RESP_EXPIRE_PACKET = 2227;

    public final static int TAIR_REQ_EXPIREAT_PACKET = 2131;
    public final static int TAIR_RESP_EXPIREAT_PACKET = 2231;

    public final static int TAIR_REQ_PERSIST_PACKET = 2132;
    public final static int TAIR_RESP_PERSIST_PACKET = 2232;

    public final static int TAIR_REQ_TTL_PACKET = 2134;
    public final static int TAIR_RESP_TTL_PACKET = 2234;

    public final static int TAIR_REQ_TYPE_PACKET = 2135;
    public final static int TAIR_RESP_TYPE_PACKET = 2235;

    public final static int TAIR_REQ_ADD_FILTER_PACKET = 2162;

    public final static int TAIR_REQ_REMOVE_FILTER_PACKET = 2163;

    public final static int TAIR_REQ_DUMP_AREA_PACKET = 2164;
    public final static int TAIR_REQ_LOAD_AREA_PACKET = 2165;

    public final static int TAIR_REQ_SET_NS_ATTR_PACKET = 2166;
    public final static int TAIR_REQ_GET_NS_ATTR_PACKET = 2167;


    // yes, as you see, for history problem, below packet conflict with
    // public final static int TAIR_RESP_ZSCORE_PACKET = 2222;
    // public final static int TAIR_RESP_ZRANGEBYSCORE_PACKET = 2223;
    // we use EngineType to slove it
    public final static int TAIR_REQ_MC_OPS_PACKET = 2222;
    public final static int TAIR_RESP_MC_OPS_PACKET = 2223;

    //for fastdump

    public static final int TAIR_REQ_QUERY_GC_STATUS_PACKET = 2502;
    public static final int TAIR_RESP_QUERY_GC_STATUS_PACKET = 2503;
    public static final int TAIR_REQ_BULK_WRITE_PACKET = 2504;
    public static final int TAIR_RESP_BULK_WRITE_PACKET = 2505;
    public static final int TAIR_REQ_QUERY_BULK_WRITE_TOKEN_PACKET = 2506;
    public static final int TAIR_RESP_QUERY_BULK_WRITE_TOKEN_PACKET = 2507;
    public static final int TAIR_REQ_BULK_WRITE_V2_PACKET = 2512;
    public static final int TAIR_RESP_BULK_WRITE_V2_PACKET = 2513;
    
    // serialize type
    public static final int TAIR_STYPE_INT = 1;
    public static final int TAIR_STYPE_STRING = 2;
    public static final int TAIR_STYPE_BOOL = 3;
    public static final int TAIR_STYPE_LONG = 4;
    public static final int TAIR_STYPE_DATE = 5;
    public static final int TAIR_STYPE_BYTE = 6;
    public static final int TAIR_STYPE_FLOAT = 7;
    public static final int TAIR_STYPE_DOUBLE = 8;
    public static final int TAIR_STYPE_BYTEARRAY = 9;
    public static final int TAIR_STYPE_SERIALIZE = 10;
    public static final int TAIR_STYPE_INCDATA = 11;
    public static final int TAIR_STYPE_MIXEDKEY = 12;

    public static final int TAIR_PACKET_HEADER_SIZE = 16;
    public static final int TAIR_PACKET_HEADER_BLPOS = 12;

    public static final short CMD_RANGE_ALL  = 1;
    public static final short CMD_RANGE_VALUE_ONLY  = 2;
    public static final short CMD_RANGE_KEY_ONLY  = 3;
    public static final short CMD_RANGE_ALL_REVERSE  = 4;
    public static final short CMD_RANGE_VALUE_ONLY_REVERSE  = 5;
    public static final short CMD_RANGE_KEY_ONLY_REVERSE  = 6;
    public static final short CMD_DEL_RANGE  = 7;
    public static final short CMD_DEL_RANGE_REVERSE  = 8;

    public static final short PREFIX_KEY_OFFSET = 22;
    public static final int PREFIX_KEY_MASK = 0x3FFFFF;

    public static final int DEFAULT_RANGE_LIMIT = 1000;

    // buffer size
    public static final int INOUT_BUFFER_SIZE = 8192;
    public static final int DEFAULT_TIMEOUT = 2000;
    public static final int DEFAULT_CS_CONN_TIMEOUT = 1000;
    public static final int DEFAULT_CS_TIMEOUT = 500;
    public static final int DEFAULT_WAIT_THREAD = 100;
    // max key size
    public static final int MAX_KEY_COUNT = 1024;

    // etc
    public static final int TAIR_DEFAULT_COMPRESSION_THRESHOLD = 8192;
    public static final int TAIR_DEFAULT_COMPRESSION_TYPE = 0;
    public static final int TAIR_COMPRESS_TYPE_NUM = 3;
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final int TAIR_KEY_MAX_LENTH = 1024; // 1KB
    public static final int TAIR_VALUE_MAX_LENGTH =1000000;

    public static final int TAIR_MAX_COUNT = 1024;
    public static final int TAIR_MALLOC_MAX = 1 << 20; // 1MB

    public static final int NAMESPACE_MAX = Short.MAX_VALUE;

    public static final int ITEM_ALL = 65537;

    public static final int Q_AREA_CAPACITY  = 1;
    public static final int Q_MIG_INFO  = 2;
    public static final int Q_DATA_SEVER_INFO  = 3;
    public static final int Q_GROUP_INFO  = 4;
    public static final int Q_STAT_INFO  = 5;

    //~ public static final int SYNC_INVALID = 0;
  //~ public static final int ASYNC_INVALID = 1;

    public final static short   NOT_CARE_VERSION  = (short)0;
  public final static int   NOT_CARE_EXPIRE   = -1;
  public final static int   CANCEL_EXPIRE     = 0;

    public final static short   COMPARE_AND_PUT_FLAG  = (short)4;


    ///////////////////constant//////////////////
	public final static int REQUEST_ENCODE_OK = 0;
	public final static int KEYTOLARGE = 1;
	public final static int VALUETOLARGE = 2;
	public final static int SERIALIZEERROR = 3;
	public final static int DATALENTOOLONG = 4;
	public final static int KEYORVALUEISNULL = 5;

  public final static int DATA_ENTRY_MAX_SIZE = 1024*1024;
  public final static int LIST_MAX_LEN = 8192;

  public final static String TAIR_MULTI_GROUPS = "groups";
  public final static String TAIR_TMP_DOWN_SERVER = "tmp_down_server";
  public final static String TAIR_GROUP_STATUS = "group_status";
  public final static String TAIR_GROUP_STATUS_ON = "on";
  public final static String TAIR_NS_STATUS = "area_status";
  public final static int TAIR_NS_STATUS_ON = 1;
  public final static int TAIR_NS_STATUS_OFF = 0;
  public final static int TAIR_NS_STATUS_INACTIVE = -1;
  public final static String TAIR_CONFIG_VALUE_DELIMITERS = "; ";

//	public final static int TAIR_SERVER_CMD_FLUSH_MMT = 1;
//	public final static int TAIR_SERVER_CMD_RESET_DB = 2;
//	public final static int TAIR_SERVER_CMD_RESET_DS = 3;
//	public final static int TAIR_SERVER_CMD_GET_GROUP_STATUS = 4;
//	public final static int TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER = 5;
//	public final static int TAIR_SERVER_CMD_SET_GROUP_STATUS = 6;
//	public final static int TAIR_SERVER_CMD_GET_NAMESPACE_STATUS = 7;
//	public final static int TAIR_SERVER_CMD_SET_NAMESPACE_STATUS = 8;
//
//  public final static int TAIR_SERVER_CMD_NEW_NAMESPACE_UNIT = 19;
//  public final static int TAIR_SERVER_CMD_ADD_NAMESPACE= 20;
//  public final static int TAIR_SERVER_CMD_DELETE_NAMESPACE_UNIT = 21;
//  public final static int TAIR_SERVER_CMD_DELETE_NAMESPACE = 22;
//  public final static int TAIR_SERVER_CMD_LINK_NAMESPACE = 23;
//  public final static int TAIR_SERVER_CMD_RESET_NAMESPACE = 24;
//  public final static int TAIR_SERVER_CMD_SWITCH_NAMESPACE = 25;

  public static enum ServerCmdType{
    TAIR_SERVER_CMD_MIN_TYPE(0),
    TAIR_SERVER_CMD_FLUSH_MMT  (1),
    TAIR_SERVER_CMD_RESET_DB (2),
    TAIR_SERVER_CMD_RESET_DS (3),
    TAIR_SERVER_CMD_GET_GROUP_STATUS (4),
    TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER (5),
    TAIR_SERVER_CMD_SET_GROUP_STATUS (6),
    TAIR_SERVER_CMD_SET_MIGRATE_WAIT_MS (7),
    TAIR_SERVER_CMD_PAUSE_GC (8),
    TAIR_SERVER_CMD_RESUME_GC (9),
    TAIR_SERVER_CMD_RELEASE_MEM (10),
    TAIR_SERVER_CMD_STAT_DB (11),
    TAIR_SERVER_CMD_SET_CONFIG (12),
    TAIR_SERVER_CMD_GET_CONFIG (13),
    TAIR_SERVER_CMD_BACKUP_DB (14),
    TAIR_SERVER_CMD_PAUSE_RSYNC (15),
    TAIR_SERVER_CMD_RESUME_RSYNC (16),
    TAIR_SERVER_CMD_START_BALANCE (17),
    TAIR_SERVER_CMD_STOP_BALANCE (18),
    TAIR_SERVER_CMD_SET_BALANCE_WAIT_MS (19),
    TAIR_SERVER_CMD_UNLOAD_BACKUPED_DB (20),
    TAIR_SERVER_CMD_CLOSE_UNUESD_BUCKETS (21),
    TAIR_SERVER_CMD_SYNC_BUCKET (22),
    TAIR_SERVER_CMD_MIGRATE_BUCKET (23),

    TAIR_SERVER_CMD_ALLOC_AREA (24),      //to cs), alloc area && add quota
    TAIR_SERVER_CMD_SET_QUOTA (25),       //to cs), modify/delete quota
    TAIR_SERVER_CMD_GET_AREA_STATUS (32), //for multigroup fastdump), to cs), get group area status
    TAIR_SERVER_CMD_SET_AREA_STATUS (33), //for multigroup fastdump), to cs), get group area status
    TAIR_SERVER_CMD_SET_AREA_MAP (37),    //to ds, map area to another
    TAIR_SERVER_CMD_GET_AREA_MAP (38),    //to ds, get area map relation
    TAIR_ADMIN_SERVER_CMD_SET_AREA_MAP (39),    //to admin server, map area to another
    TAIR_ADMIN_SERVER_CMD_GET_AREA_MAP (40),    //to admin server, get area map relation
    TAIR_SERVER_CMD_CLEAR_MDB (43),       //to ldb), clear embeded mdb cache

    TAIR_ADMIN_SERVER_CMD_ALLOC_AREA_RING (54),      //to admin server, allow area ring
    TAIR_ADMIN_SERVER_CMD_GET_MASTER_AREA_LIST (55), //to admin server, obtain area list of the ring
    TAIR_ADMIN_SERVER_CMD_GET_AREA_RING (56),        //to admin server, obtain the area ring
    TAIR_ADMIN_SERVER_CMD_GET_NEXT_AREAMAP (57),     //to admin server, obtain the next area in the area ring
    TAIR_ADMIN_SERVER_CMD_GET_PREV_AREAMAP (58),     //to admin server, obtain the previous area in the area ring
    TAIR_SERVER_CMD_CLEAN_MD5_CACHE (71),            //to data server, clean md5 cache
    TAIR_SERVER_CMD_DROP_AREA_DATA (131),            //to data server, drop all data of the namespace.
    // all cmd type should be less TAIR_SERVER_CMD_MAX_TYPE
    //TAIR_SERVER_CMD_MAX_TYPE
    ;

    ServerCmdType(int id)
      {
          id_ = id;
      }

      public int value()
      {
          return id_;
      }

      private final int id_;
  }

}
