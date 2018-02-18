/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.fastdump;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.CallMode;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairCallback;
import com.taobao.tair.TairManager;
import com.taobao.tair.comm.DataEntryLocalCache;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.CounterPack;
import com.taobao.tair.etc.KeyCountPack;
import com.taobao.tair.etc.KeyValuePack;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.impl.ConfigServer;
import com.taobao.tair.impl.DefaultTairManager;

public class FastDumpTairManager implements TairManager {
	private static final Logger log = LoggerFactory.getLogger(FastDumpTairManager.class);
	private static final String clientVersion = "TairClient-FastDump 1.0.0";
	private ClusterInfo clusterInfo = new ClusterInfo();
	private TairManager tairManager = null;

  public static String getClientVersion() {
    return clientVersion;
  }

  public void setConfigServerList(List<String> configServerList) {
    clusterInfo.setConfigServerList(configServerList);
  }

  public List<String> getConfigServerList() {
    return clusterInfo.getConfigServerList();
  }

  public void setGroupName(String groupName) {
    clusterInfo.setGroupName(groupName);
  }

  public String getGroupName() {
    return clusterInfo.getGroupName();
  }

  public void setMaxFailCount(int failCount) {
    clusterInfo.setMaxFailCount(failCount);
    if (tairManager != null)
    	tairManager.setMaxFailCount(failCount);
  }

  public int getMaxFailCount() {
	  if (tairManager != null) {
		  return tairManager.getMaxFailCount();
	  }
    return clusterInfo.getMaxFailCount();
  }

  public void setTimeout(int timeout) {
    clusterInfo.setTimeout(timeout);
    if (tairManager != null)
    	tairManager.setTimeout(timeout);
  }

	public int getTimeout() {
		if (tairManager != null) {
			return tairManager.getTimeout();
		}
        return clusterInfo.getTimeout();
    }

	public void setCompressionThreshold(int compressionThreshold) {
		clusterInfo.setCompressionThreshold(compressionThreshold);
		if (tairManager != null) {
			tairManager.setCompressionThreshold(compressionThreshold);
		}
	}

  public int getCompressionThreshold() {
	  if (tairManager != null) {
		  tairManager.getCompressionThreshold();
	  }
    return clusterInfo.getCompressionThreshold();
  }

  public String getCharset() {
	  if (tairManager != null) {
		  tairManager.getCharset();
	  }
    return clusterInfo.getCharset();
  }

    public boolean getHeader() {
        return clusterInfo.getHeader();
    }

    public void setHeader(boolean header) {
        clusterInfo.setHeader(header);
        if (tairManager != null) {
        	tairManager.setHeader(header);
        }
    }

  public void setCharset(String charset) {
    clusterInfo.setCharset(charset);
    if (tairManager != null) {
    	tairManager.setCharset(charset);
    }
  }

  public void init() {
    // init cluster handler manager
    tairManager = initTairManager(this.clusterInfo);
  }

  public void close() {
    // destroy cluster handler manager
    destroyTairManager(this.clusterInfo);
  }

  public void destroy(){

  }

  public Result<DataEntry> get(int namespace, Serializable key) {
    return tairManager.get(namespace, key);
  }

  public Result<List<DataEntry>> mget(int namespace, List<? extends Object> keys) {
    return tairManager.mget(namespace, keys);
  }

  public ResultCode put(int namespace, Serializable key, Serializable value) {
    return put(namespace, key, value, 0, 0, true);
  }

  public ResultCode put(int namespace, Serializable key, Serializable value,
      int version) {
    return put(namespace, key, value, version, 0, true);
  }

  public ResultCode put(int namespace, Serializable key, Serializable value,
      int version, int expireTime) {
    return put(namespace, key, value, version, expireTime, true);
  }

  public ResultCode put(int namespace, Serializable key, Serializable value,
      int version, int expireTime, boolean fillCache) {
    return tairManager.put(namespace, key, value, version, expireTime, fillCache);
  }

  public ResultCode putAsync(int namespace, Serializable key, Serializable value,
      int version, int expireTime, boolean fillCache, TairCallback cb) {
    return tairManager.putAsync(namespace, key, value, version, expireTime, fillCache, cb);
  }

  public ResultCode mput(int namespace, List<KeyValuePack> kvPacks) {
    return mput(namespace, kvPacks, true);
  }

  public ResultCode mput(int namespace, List<KeyValuePack> kvPacks, boolean compress) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).mput(namespace, kvPacks, compress);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  public ResultCode compareAndPut(int namespace, Serializable key, Serializable value, short epoch) {
      return compareAndPut(namespace, key, value, epoch, 0);
  }

  public ResultCode compareAndPut(int namespace, Serializable key, Serializable value, short epoch, int expireTime) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).compareAndPut(namespace, key, value, epoch, expireTime);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).compareAndPut(namespace, key, value, epoch, expireTime);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  public ResultCode delete(int namespace, Serializable key) {
    return tairManager.delete(namespace, key);
  }

  public ResultCode invalid(int namespace, Serializable key) {
    return tairManager.invalid(namespace, key);
  }

  public ResultCode invalid(int namespace, Serializable key, CallMode callMode) {
    return tairManager.invalid(namespace, key, callMode);
  }

  public ResultCode hide(int namespace, Serializable key) {
    return tairManager.hide(namespace, key);
  }

  public ResultCode hideByProxy(int namespace, Serializable key) {
    return tairManager.hideByProxy(namespace, key);
  }

  public ResultCode hideByProxy(int namespace, Serializable key, CallMode callMode) {
    return tairManager.hideByProxy(namespace, key, callMode);
  }

  public Result<DataEntry> getHidden(int namespace, Serializable key) {
    return tairManager.getHidden(namespace, key);
  }

  public Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit, boolean reverse){
		return tairManager.getRange(namespace, prefix, key_start, key_end, offset, limit, reverse);
    }

    public Result<List<DataEntry>> getRangeOnlyValue(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit, boolean reverse){
		return tairManager.getRangeOnlyValue(namespace, prefix, key_start, key_end, offset, limit, reverse);
    }

    public Result<List<DataEntry>> getRangeOnlyKey(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit, boolean reverse){
		return tairManager.getRangeOnlyKey(namespace, prefix, key_start, key_end, offset, limit, reverse);
    }

    public Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit){
		return tairManager.getRange(namespace, prefix, key_start, key_end, offset, limit, false);
    }

  public Result<List<DataEntry>> getRangeOnlyValue(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit){
		return tairManager.getRangeOnlyValue(namespace, prefix, key_start, key_end, offset, limit, false);
    }

  public Result<List<DataEntry>> getRangeOnlyKey(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit){
		return tairManager.getRangeOnlyKey(namespace, prefix, key_start, key_end, offset, limit, false);
    }

    public Result<List<DataEntry>> delRange(int namespace, Serializable prefix,
      Serializable key_start, Serializable key_end, int offset, int limit, boolean reverse){
		return tairManager.delRange(namespace, prefix, key_start, key_end, offset, limit, reverse);
    }

    public ResultCode prefixPut(int namespace, Serializable pkey, Serializable skey, Serializable value) {
		return tairManager.prefixPut(namespace, pkey, skey, value);
	}

  public ResultCode prefixPut(int namespace, Serializable pkey, Serializable skey, Serializable value, int version) {
    return tairManager.prefixPut(namespace, pkey, skey, value, version);
  }

  public ResultCode prefixPut(int namespace, Serializable pkey, Serializable skey, Serializable value, int version, int expireTime) {
    return tairManager.prefixPut(namespace, pkey, skey, value, version, expireTime);
  }

  public Result<Map<Object, ResultCode>> prefixPuts(int namespace, Serializable pkey, List<KeyValuePack> keyValuePacks) {
    return tairManager.prefixPuts(namespace, pkey, keyValuePacks);
  }

  public Result<DataEntry> prefixGet(int namespace, Serializable pkey, Serializable skey) {
    return tairManager.prefixGet(namespace, pkey, skey);
  }

  public Result<Map<Object, Result<DataEntry>>> prefixGets(int namespace, Serializable pkey, List<? extends Serializable> skeyList) {
    return tairManager.prefixGets(namespace, pkey, skeyList);
  }

  public ResultCode prefixDelete(int namespace, Serializable pkey, Serializable skey) {
    return tairManager.prefixDelete(namespace, pkey, skey);
  }

  public Result<Map<Object, ResultCode>> prefixDeletes(int namespace, Serializable pkey, List<? extends Serializable> skeys) {
    return tairManager.prefixDeletes(namespace, pkey, skeys);
  }

  public Result<Integer> prefixIncr(int namespace, Serializable pkey, Serializable skey, int value, int defaultValue, int expireTime) {
    return tairManager.prefixIncr(namespace, pkey, skey, value, defaultValue, expireTime);
  }

  public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace, Serializable pkey, List<CounterPack> packList) {
    return tairManager.prefixIncrs(namespace, pkey, packList);
  }

  public Result<Integer> prefixDecr(int namespace, Serializable pkey, Serializable skey, int value, int defaultValue, int expireTime) {
    return tairManager.prefixDecr(namespace, pkey, skey, value, defaultValue, expireTime);
  }

  public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace, Serializable pkey, List<CounterPack> packList) {
    return tairManager.prefixDecrs(namespace, pkey, packList);
  }

  public ResultCode prefixSetCount(int namespace, Serializable pkey, Serializable skey, int count) {
    return tairManager.prefixSetCount(namespace, pkey, skey, count);
  }

  public ResultCode prefixSetCount(int namespace, Serializable pkey, Serializable skey, int count, int version, int expireTime) {
    return tairManager.prefixSetCount(namespace, pkey, skey, count, version, expireTime);
  }

  public ResultCode prefixHide(int namespace, Serializable pkey, Serializable skey) {
    return tairManager.prefixHide(namespace, pkey, skey);
  }

  public Result<DataEntry> prefixGetHidden(int namespace, Serializable pkey, Serializable skey) {
    return tairManager.prefixGetHidden(namespace, pkey, skey);
  }

  public Result<Map<Object, Result<DataEntry>>>
    prefixGetHiddens(int namespace, Serializable pkey, List<? extends Serializable> skeys) {
      return tairManager.prefixGetHiddens(namespace, pkey, skeys);
    }

  public Result<Map<Object, ResultCode>> prefixHides(int namespace, Serializable pkey, List<? extends Serializable> skeys) {
    return tairManager.prefixHides(namespace, pkey, skeys);
  }

  public ResultCode prefixInvalid(int namespace, Serializable pkey, Serializable skey, CallMode callMode) {
    return tairManager.prefixInvalid(namespace, pkey, skey, callMode);
  }

  public ResultCode prefixHideByProxy(int namespace, Serializable pkey, Serializable skey, CallMode callMode) {
    return tairManager.prefixHideByProxy(namespace, pkey, skey, callMode);
  }

  public Result<Map<Object, ResultCode>> prefixHidesByProxy(int namespace, Serializable pkey, List<? extends Serializable> skeys, CallMode callMode) {
    return tairManager.prefixHidesByProxy(namespace, pkey, skeys, callMode);
  }

  public Result<Map<Object, ResultCode>> prefixInvalids(int namespace, Serializable pkey, List<? extends Serializable> skeys, CallMode callMode) {
    return tairManager.prefixInvalids(namespace, pkey, skeys, callMode);
  }

  public Result<Map<Object, Map<Object, Result<DataEntry>>>>
        mprefixGets(int namespace, Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap ) {
		if (tairManager instanceof DefaultTairManager)
			return ((DefaultTairManager)tairManager).mprefixGets(namespace, pkeySkeyListMap);
		else if (tairManager instanceof DefaultFastDumpTairManager)
			return ((DefaultFastDumpTairManager)tairManager).mprefixGets(namespace, pkeySkeyListMap);
		return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(ResultCode.CONNERROR);
	}

  public Result<Map<Object, Map<Object, Result<DataEntry>>>>
    mprefixGetHiddens(int namespace, Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap ) {
      return tairManager.mprefixGetHiddens(namespace, pkeySkeyListMap);
    }

  public ResultCode minvalid(int namespace, List<? extends Object> keys) {
    return tairManager.minvalid(namespace, keys);
  }

  public ResultCode mdelete(int namespace, List<? extends Object> keys) {
    return tairManager.mdelete(namespace, keys);
  }

  public Result<Integer> incr(int namespace, Serializable key, int value,
      int defaultValue, int expireTime) {
    return tairManager.incr(namespace, key, value, defaultValue, expireTime);
  }

  public Result<Integer> decr(int namespace, Serializable key, int value,
      int defaultValue, int expireTime) {
    return tairManager.decr(namespace, key, value, defaultValue, expireTime);
  }

  public ResultCode setCount(int namespace, Serializable key, int count) {
    return tairManager.setCount(namespace, key, count);
  }

  public ResultCode setCount(int namespace, Serializable key, int count, int version, int expireTime) {
    return tairManager.setCount(namespace, key, count, version, expireTime);
  }

  public ResultCode addItems(int namespace, Serializable key,
      List<? extends Object> items, int maxCount, int version,
      int expireTime) {
    return tairManager.addItems(namespace, key, items, maxCount, version, expireTime);
  }

  public Result<DataEntry> getItems(int namespace, Serializable key,
      int offset, int count) {
    return tairManager.getItems(namespace, key, offset, count);
  }

  public ResultCode removeItems(int namespace, Serializable key, int offset,
      int count) {
    return tairManager.removeItems(namespace, key, offset, count);
  }

  public Result<DataEntry> getAndRemove(int namespace,
      Serializable key, int offset, int count) {
    return tairManager.getAndRemove(namespace, key, offset, count);
  }

  public Result<Integer> getItemCount(int namespace, Serializable key) {
    return tairManager.getItemCount(namespace, key);
  }

  public ResultCode lock(int namespace, Serializable key) {
    return tairManager.lock(namespace, key);
  }

  public ResultCode unlock(int namespace, Serializable key) {
    return tairManager.unlock(namespace, key);
  }

  public Result<List<Object>> mlock(int namespace, List<? extends Object> keys) {
    return tairManager.mlock(namespace, keys);
  }

  public Result<List<Object>> mlock(int namespace, List<? extends Object> keys, Map<Object, ResultCode> failKeysMap) {
    return tairManager.mlock(namespace, keys, failKeysMap);
  }

  public Result<List<Object>> munlock(int namespace, List<? extends Object> keys) {
    return tairManager.munlock(namespace, keys);
  }

  public Result<List<Object>> munlock(int namespace, List<? extends Object> keys, Map<Object, ResultCode> failKeysMap) {
    return tairManager.munlock(namespace, keys, failKeysMap);
  }

  public Map<String,String> getStat(int qtype, String groupName, long serverId) {
    return tairManager.getStat(qtype, groupName, serverId);
  }

  public String getVersion() {
    return tairManager.getVersion();
  }

  public int getBucketCount() {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).getBucketCount();
    } else {
      return -1;
    }
  }

  public int getBucketOfKey(Serializable key) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).getBucketOfKey(key);
    } else {
      return -1;
    }
  }

  public String getGroupStatus(String group) {
    if (group == null) {
      return null;
    }
    List<String> groups = new ArrayList<String>();
    groups.add(group);
    List<String> status = getGroupStatus(groups);
    if (status != null) {
      return status.get(0);
    }
    return null;
  }

  public List<String> getGroupStatus(List<String> groups) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).getGroupStatus(groups);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).getGroupStatus(groups);
    }
    return null;
  }

  public ResultCode setGroupStatus(String group, String status) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).setGroupStatus(group, status);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).setGroupStatus(group, status);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  //get all namespace status
  public List<String> getNsStatus(List<String> groups) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).getNsStatus(groups);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).getNsStatus(groups);
    }
    return null;
  }

  public ResultCode setNsStatus(String group, int namespace, String status) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).setNsStatus(group, namespace, status);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).setNsStatus(group, namespace, status);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  public String getTmpDownServer(String group) {
    if (group == null) {
      return null;
    }
    List<String> groups = new ArrayList<String>();
    groups.add(group);
    List<String> tmpDownServers = getTmpDownServer(groups);
    if (tmpDownServers != null) {
      return tmpDownServers.get(0);
    }
    return null;
  }

  public List<String> getTmpDownServer(List<String> groups) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).getTmpDownServer(groups);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).getTmpDownServer(groups);
    }
    return null;
  }

  public ResultCode resetServer(String group) {
    return resetServer(group, null);
  }

  public ResultCode resetServer(String group, String dataServer) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).resetServer(group, dataServer);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).resetServer(group, dataServer);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  // cmd to dataserver
  public ResultCode flushMmt(String group, int namespace) {
    return flushMmt(group, null, namespace);
  }

  public ResultCode flushMmt(String group, String dataServer, int namespace) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).flushMmt(group, dataServer, namespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).flushMmt(group, dataServer, namespace);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  public ResultCode resetDb(String group, int namespace) {
    return resetDb(group, null, namespace);
  }

  public ResultCode resetDb(String group, String dataServer, int namespace) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).resetDb(group, dataServer, namespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).resetDb(group, dataServer, namespace);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }


  /**
   * get current namespace actually pointting to.
   * @param namespace: namespace for read
   * @return namespace for write
   */
  public int getMapedNamespace(int namespace) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).getMapedNamespace(namespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).getMapedNamespace(namespace);
    }
    log.error("Not supported manager");
    return -1;
  }

  /**
   * map a namespace for read.
   * @param namespace: namespace for read
   * @param dumpnamespace: namespace to be switching
   */
  public ResultCode mapNamespace(int namespace, int dumpNamespace) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).mapNamespace(namespace, dumpNamespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).mapNamespace(namespace, dumpNamespace);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  /**
   * get a new namespace for dump. (and clear it)
   * @param namespace: namespace for read
   * @return new namespace for dump or -1 (failed)
   */
  public int resetNamespace(int namespace) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).resetNamespace(namespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).resetNamespace(namespace);
    }
    return -1;
  }

  /**
   * rollback to a old namespace for read.
   * @param namespace: namespace for read
   * @return old namespace
   */
  public synchronized int rollbackNamespace(int namespace) {
    if (tairManager instanceof DefaultTairManager) {
      return ((DefaultTairManager)tairManager).rollbackNamespace(namespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).rollbackNamespace(namespace);
    }
    return -1;
  }

  /**
   * init namespace ring for fastdump
   * @param namespace: namespace to init
   * @param backupCount: how many namespace to reserved for this namespace
   *                for example:  initNamespace(1, 3); maybe alloc 1 -> (2048, 2049, 2050), and map 1 to 2048
   * @return true or false
   */

  public synchronized ResultCode initNamespace(int namespace, int backupCount) {
    if (tairManager instanceof DefaultTairManager) {
      return ResultCode.TAIR_IS_NOT_SUPPORT;
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).initNamespace(namespace, backupCount);
    }
    return ResultCode.TAIR_IS_NOT_SUPPORT;
  }

  public static TairManager initTairManager(ClusterInfo clusterInfo) {
    DefaultTairManager manager = new DefaultTairManager();
    manager.setForceService(true);
    manager.setConfigServerList(clusterInfo.getConfigServerList());
    manager.setGroupName(clusterInfo.getGroupName());
    manager.setCompressionThreshold(clusterInfo.getCompressionThreshold());
    manager.setHeader(clusterInfo.getHeader());
    manager.init();
    Map<String, String> configMap = manager.retrieveConfigMap();
    if (configMap == null) {
      manager.close();
      throw new RuntimeException("init TairManager get configMap fail");
    }

    List<String> groups = TairUtil.parseConfig(configMap, TairConstant.TAIR_MULTI_GROUPS,
        TairConstant.TAIR_CONFIG_VALUE_DELIMITERS);
    // This is a FastDump cluster.
    if (groups != null) {
      log.warn("init DefaultFastDumpTairManager"+ groups);
      DefaultFastDumpTairManager fastDumpManager = new DefaultFastDumpTairManager();
      fastDumpManager.setClusterInfo(clusterInfo);
      fastDumpManager.init();
      manager.close();
      return fastDumpManager;
    } else {
      manager.setForceService(false);
      return manager;
    }
  }

  public void destroyTairManager(ClusterInfo clusterInfo) {
    if (tairManager instanceof DefaultTairManager) {
      ((DefaultTairManager)tairManager).close();
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      ((DefaultFastDumpTairManager)tairManager).close();
    }
  }

  public synchronized void setupLocalCache(int namespace) {
  	setupLocalCache(namespace, 30, 30);
  }

  /*
   * cap: kv capcity, item count in localcache
   * exp: expiration in localcache   (ms)
   */
  public synchronized void setupLocalCache(int namespace, int cap, long exp) {
    if (tairManager instanceof DefaultTairManager) {
      ((DefaultTairManager)tairManager).setupLocalCache(namespace, cap, exp);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      ((DefaultFastDumpTairManager)tairManager).setupLocalCache(namespace, cap, exp);
    }
  }

  public synchronized void destroyLocalCache(int namespace) {
    if (tairManager instanceof DefaultTairManager) {
      ((DefaultTairManager)tairManager).destroyLocalCache(namespace);
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      ((DefaultFastDumpTairManager)tairManager).destroyLocalCache(namespace);
    }
  }

  public synchronized void destroyAllLocalCache() {
    if (tairManager instanceof DefaultTairManager) {
      ((DefaultTairManager)tairManager).destroyAllLocalCache();
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      ((DefaultFastDumpTairManager)tairManager).destroyAllLocalCache();
    }
  }

  /**
   * @see com.taobao.tair.TairManager#get(int, java.io.Serializable, int)
   */
  public Result<DataEntry> get(int namespace, Serializable key, int expireTime) {
      return new Result<DataEntry>(ResultCode.TAIR_IS_NOT_SUPPORT);
  }

	public Result<Map<Object, ResultCode>> prefixPuts(int namespace,
			Serializable pkey, List<KeyValuePack> keyValuePacks,
			List<KeyCountPack> keyCountPacks) {
        return new Result<Map<Object, ResultCode>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

    public Result<Integer> incr(int namespace, Serializable key, int value,
            int defaultValue, int expireTime, int lowBound, int upperBound) {
        return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
    }

	public Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBound, int upperBound) {
        return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> prefixIncr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBound, int upperBound) {
		// TODO Auto-generated method stub
		return null;
	}

	public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound) {
		// TODO Auto-generated method stub
		return null;
	}

	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBound, int upperBound) {
		// TODO Auto-generated method stub
		return null;
	}

	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound) {
		// TODO Auto-generated method stub
		return null;
	}

	public Result<Map<Object, ResultCode>> prefixSetCounts(int namespace,
			Serializable pkey, List<KeyCountPack> keyCountPacks) {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultCode append(int namespace, byte[] key, byte[] value) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public void setupLocalCache(int namespace, int cap, long exp, ClassLoader cl) {
	}

	public void enhanceLocalCache(int namespace) {
	}

	public DataEntryLocalCache getLocalCache(int namespace) {
		return null;
	}

	public Map<Integer, DataEntryLocalCache> getAllLocalCache() {
		return null;
	}

	public void setSupportBackupMode(boolean supportBackupMode) {
	}

	public int getCompressionType() {
		return 0;
	}

	public void setCompressionType(int compressionType) {
	}

	public void setDataServer(String dataServer) {

	}

	public int getMaxWaitThread() {
		return 0;
	}

	public void setAdminTimeout(int timeout) {
	}

	public ConfigServer getConfigServer() {
		return null;
	}

	public void setMaxWaitThread(int maxThreadCount) {
	}

	public ClassLoader getCustomClassLoader() {
		return null;
	}

	public void setCustomClassLoader(ClassLoader customClassLoader) {
	}

	public Map<Long, Set<Serializable>> classifyKeys(
			Collection<? extends Serializable> keys)
			throws IllegalArgumentException {
		return null;
	}

	public int getNamespaceOffset() {
		return 0;
	}

	public void setNamespaceOffset(int namespaceOffset) {
	}

	public void setRefluxRatio(int ratio) {
	}

	public void resetHappendDownServer() {
	}

	public Map<String, String> notifyStat() {
		return null;
	}

	public Transcoder getTranscoder() {
		return null;
	}

	public ResultCode lazyRemoveArea(int namespace) {
		return null;
	}

  public boolean setFastdumpNamespaceGroupNum(int num) {
    return false;
  }

  public List<String> getIpOfKey(Object key) {
    if (tairManager instanceof DefaultTairManager) {
      List<String> ips = new ArrayList<String>();
      ips.add(((DefaultTairManager)tairManager).getIpOfKey(key));
      return ips;
    } else if (tairManager instanceof DefaultFastDumpTairManager) {
      return ((DefaultFastDumpTairManager)tairManager).getIpOfKey(key);
    }
    log.error("tairmanager is not surported");
    return null;
  }

	public Result<Map<Object, Result<DataEntry>>> simplePrefixGets(
			int namespace, Serializable pkey,
			List<? extends Serializable> subkeys) {
		return null;
	}

	public String getConfigId() {
		if (this.tairManager != null)
			return this.tairManager.getConfigId();
		return null;
	}
}
