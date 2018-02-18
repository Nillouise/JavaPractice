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
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

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
import com.taobao.tair.impl.ConfigServer;

public class DefaultFastDumpTairManager implements TairManager {
	private static final Logger log = LoggerFactory
			.getLogger(DefaultFastDumpTairManager.class);
	private ClusterInfo clusterInfo = null;
	private ClusterInfoUpdater infoUpdater = new ClusterInfoUpdater();
	private ClusterHandlerManager handlerManager;
	private String clusterShardStrategy = "bucket-hash";
	private String clientVersion = "FastDumpTairManager-1.1.0";

	public void init() {
		infoUpdater.setClusterInfo(clusterInfo);
		if (!infoUpdater.init()) {
			throw new RuntimeException(
					"init init DefaultFastDumpTairManager fail: init updater fail");
		}

		initClusterHandlerManager();
		infoUpdater.setClusterHandlerManager(handlerManager);

		if (!infoUpdater.forceUpdate()) {
			throw new RuntimeException(
					"init DefaultFastDumpTairManager fail: forceUpdate fail");
		}
		if (!handlerManager.canService()) {
			throw new RuntimeException(
					"NO cluster can service now. maybe all cluster is OFF or all server is down(not reset)");
		}

		infoUpdater.start();
	}

	public void close() {
		infoUpdater.close();
		infoUpdater = null;
		handlerManager.close();
		handlerManager = null;
		clusterInfo = null;
	}

	public Result<DataEntry> get(int namespace, Serializable key) {
		ClusterHandler handler = handlerManager.pickHandler(key, namespace);
		if (handler != null) {
			Result<DataEntry> result = handler.getTairManager().get(namespace,
					key);
			updateConfigVersion(handler);
			return result;
		}
		return new Result<DataEntry>(ResultCode.CLIENTHANDLERERR);
	}

	public Result<List<DataEntry>> mget(int namespace,
			List<? extends Object> keys) {
		// based on first key
		// TODO ..
		if (keys == null || keys.isEmpty()) {
			return new Result<List<DataEntry>>(ResultCode.INVALIDARG);
		}
		ClusterHandler handler = handlerManager.pickHandler(
				(Serializable) keys.get(0), namespace);
		if (handler != null) {
			Result<List<DataEntry>> result = handler.getTairManager().mget(
					namespace, keys);
			updateConfigVersion(handler);
			return result;
		}

		return new Result<List<DataEntry>>(ResultCode.CLIENTHANDLERERR);
	}

	public int resetNamespace(int namespace) {
		ResultCode rc = ResultCode.SUCCESS;
		int realNamespace = -2;
		int dumpNamespace = -2;
		int tmpNs, tmpDumpNs;
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			tmpNs = handler.getTairManager().getMapedNamespace(namespace);
			if (realNamespace != -2 && tmpNs != realNamespace) {
				log.error("cluster map inconsistent:" + namespace + "->"
						+ realNamespace + " != " + namespace + "->" + tmpNs
						+ ". please comfirm and map it again");
				rc = ResultCode.SERVERERROR;
				break;
			}
			realNamespace = tmpNs;
			tmpDumpNs = handler.getTairManager().resetNamespace(namespace);
			log.error(handler.getTairManager().getGroupName() + " reset to "
					+ tmpDumpNs);
			if (dumpNamespace != -2 && tmpDumpNs != dumpNamespace) {
				log.error("cluster map inconsistent:" + namespace + "->"
						+ dumpNamespace + " != " + namespace + "->" + tmpDumpNs
						+ ". please comfirm setFastdumpNamespaceGroupNum");
				rc = ResultCode.SERVERERROR;
				break;
			}
			dumpNamespace = tmpDumpNs;
			log.error("reset group:" + handler.getTairManager().getGroupName()
					+ " " + namespace + "->" + dumpNamespace);
		}
		if (rc.getCode() == ResultCode.SUCCESS.getCode() && dumpNamespace != -2) {
			return dumpNamespace;
		} else {
			return -1;
		}
	}

	public ResultCode mapNamespace(int namespace, int dumpNamespace) {
		ResultCode rc = ResultCode.CLIENTHANDLERERR;
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			rc = handler.getTairManager()
					.mapNamespace(namespace, dumpNamespace);
			if (!rc.isSuccess()) {
				return rc;
			}
		}
		return rc;
	}

	public int getMapedNamespace(int namespace) {
		ResultCode rc = ResultCode.SUCCESS;
		int realNamespace = -1;
		int tmpNs;
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			tmpNs = handler.getTairManager().getMapedNamespace(namespace);
			if (realNamespace != -1 && tmpNs != realNamespace) {
				log.error("cluster map inconsistent:" + namespace + "->"
						+ realNamespace + " != " + namespace + "->" + tmpNs
						+ ". please comfirm and map it again");
				rc = ResultCode.SERVERERROR;
				break;
			}
			realNamespace = tmpNs;
		}
		if (rc.getCode() == ResultCode.SUCCESS.getCode() && realNamespace != -1) {
			return realNamespace;
		} else {
			return -1;
		}
	}

	public int rollbackNamespace(int namespace) {
		ResultCode rc = ResultCode.SUCCESS;
		int realNamespace = -2;
		int rollbackNamespace = -2;
		int tmpNs, tmpRbNs;
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			tmpNs = handler.getTairManager().getMapedNamespace(namespace);
			if (realNamespace != -2 && tmpNs != realNamespace) {
				log.error("cluster map inconsistent:" + namespace + "->"
						+ realNamespace + " != " + namespace + "->" + tmpNs
						+ ".");
				rc = ResultCode.SERVERERROR;
				break;
			}
			realNamespace = tmpNs;
			tmpRbNs = handler.getTairManager().rollbackNamespace(namespace);
			log.error(handler.getTairManager().getGroupName() + " reset to "
					+ tmpRbNs);
			if (rollbackNamespace != -2 && tmpRbNs != rollbackNamespace) {
				log.error("cluster map inconsistent:" + namespace + "->"
						+ rollbackNamespace + " != " + namespace + "->"
						+ tmpRbNs + ".");
				rc = ResultCode.SERVERERROR;
				break;
			}
			rollbackNamespace = tmpRbNs;
			log.error("reset group:" + handler.getTairManager().getGroupName()
					+ " " + namespace + "->" + rollbackNamespace);
		}
		if (rc.getCode() == ResultCode.SUCCESS.getCode()
				&& rollbackNamespace != -2) {
			return rollbackNamespace;
		} else {
			return -1;
		}
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
		ResultCode rc = ResultCode.CLIENTHANDLERERR;
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			rc = handler.getTairManager().put(namespace, key, value, version,
					expireTime, fillCache);
			if (!rc.isSuccess()) {
				return rc;
			}
			int newVersion = handler.getConfigVersion();
			if (newVersion != handler.getLastVersion()) {
				handlerManager.update(true);
				handler.setLastVersion(newVersion);
			}
		}
		return rc;
	}

	public ResultCode putAsync(int namespace, Serializable key,
			Serializable value, int version, int expireTime, boolean fillCache,
			TairCallback cb) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode mput(int namespace, List<KeyValuePack> kvPacks) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode mput(int namespace, List<KeyValuePack> kvPacks,
			boolean compress) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode compareAndPut(int namespace, Serializable key,
			Serializable value, short epoch) {
		return compareAndPut(namespace, key, value, epoch, 0);
	}

	public ResultCode compareAndPut(int namespace, Serializable key,
			Serializable value, short epoch, int expireTime) {
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			ResultCode rc = handler.getTairManager().compareAndPut(namespace,
					key, value, epoch, expireTime);
			if (!rc.isSuccess()) {
				return rc;
			}
			int newVersion = handler.getConfigVersion();
			if (newVersion != handler.getLastVersion()) {
				handlerManager.update(true);
				handler.setLastVersion(newVersion);
			}
		}
		return ResultCode.SUCCESS;
	}

	public ResultCode delete(int namespace, Serializable key) {
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			ResultCode result = handler.getTairManager().delete(namespace, key);
			if (!result.isSuccess()) {
				return result;
			}
			int newVersion = handler.getConfigVersion();
			if (newVersion != handler.getLastVersion()) {
				handlerManager.update(true);
				handler.setLastVersion(newVersion);
			}
		}
		return ResultCode.SUCCESS;
	}

	public ResultCode invalid(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode invalid(int namespace, Serializable key, CallMode callMode) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode hide(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode hideByProxy(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode hideByProxy(int namespace, Serializable key,
			CallMode callMode) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<DataEntry> getHidden(int namespace, Serializable key) {
		return new Result<DataEntry>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset, int limit) {
		return getRange(namespace, prefix, key_start, key_end, offset, limit,
				false);
	}

	public Result<List<DataEntry>> getRangeOnlyValue(int namespace,
			Serializable prefix, Serializable key_start, Serializable key_end,
			int offset, int limit) {
		return getRangeOnlyValue(namespace, prefix, key_start, key_end, offset,
				limit, false);
	}

	public Result<List<DataEntry>> getRangeOnlyKey(int namespace,
			Serializable prefix, Serializable key_start, Serializable key_end,
			int offset, int limit) {
		return getRangeOnlyKey(namespace, prefix, key_start, key_end, offset,
				limit, false);
	}

	public Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset,
			int limit, boolean reverse) {
		ClusterHandler handler = handlerManager.pickHandler(prefix, namespace);
		if (handler != null) {
			Result<List<DataEntry>> result = handler.getTairManager().getRange(
					namespace, prefix, key_start, key_end, offset, limit,
					reverse);
			updateConfigVersion(handler);
			return result;
		}
		return new Result<List<DataEntry>>(ResultCode.CLIENTHANDLERERR);
	}

	public Result<List<DataEntry>> getRangeOnlyValue(int namespace,
			Serializable prefix, Serializable key_start, Serializable key_end,
			int offset, int limit, boolean reverse) {
		ClusterHandler handler = handlerManager.pickHandler(prefix, namespace);
		if (handler != null) {
			Result<List<DataEntry>> result = handler.getTairManager()
					.getRangeOnlyValue(namespace, prefix, key_start, key_end,
							offset, limit, reverse);
			updateConfigVersion(handler);
			return result;
		}
		return new Result<List<DataEntry>>(ResultCode.CLIENTHANDLERERR);
	}

	public Result<List<DataEntry>> getRangeOnlyKey(int namespace,
			Serializable prefix, Serializable key_start, Serializable key_end,
			int offset, int limit, boolean reverse) {
		ClusterHandler handler = handlerManager.pickHandler(prefix, namespace);
		if (handler != null) {
			Result<List<DataEntry>> result = handler.getTairManager()
					.getRangeOnlyKey(namespace, prefix, key_start, key_end,
							offset, limit, reverse);
			updateConfigVersion(handler);
			return result;
		}
		return new Result<List<DataEntry>>(ResultCode.CLIENTHANDLERERR);
	}

	public Result<List<DataEntry>> delRange(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset,
			int limit, boolean reverse) {
		return new Result<List<DataEntry>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value) {
		return prefixPut(namespace, pkey, skey, 0, 0);
	}

	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value, int version) {
		return prefixPut(namespace, pkey, skey, version, 0);
	}

	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value, int version, int expireTime) {
		ClusterHandler handler = handlerManager.pickHandler(pkey, namespace);
		if (handler != null) {
			ResultCode result = handler.getTairManager().prefixPut(namespace,
					pkey, skey, value, version, expireTime);
			updateConfigVersion(handler);
			return result;
		}
		return ResultCode.CLIENTHANDLERERR;
	}

	public Result<Map<Object, ResultCode>> prefixPuts(int namespace,
			Serializable pkey, List<KeyValuePack> keyValuePacks) {
		return new Result<Map<Object, ResultCode>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<DataEntry> prefixGet(int namespace, Serializable pkey,
			Serializable skey) {
		ClusterHandler handler = handlerManager.pickHandler(pkey, namespace);
		if (handler != null) {
			Result<DataEntry> result = handler.getTairManager().prefixGet(
					namespace, pkey, skey);
			updateConfigVersion(handler);
			return result;
		}
		return new Result<DataEntry>(ResultCode.CLIENTHANDLERERR);
	}

	public Result<Map<Object, Result<DataEntry>>> prefixGets(int namespace,
			Serializable pkey, List<? extends Serializable> skeyList) {
		ClusterHandler handler = handlerManager.pickHandler(pkey, namespace);
		if (handler != null) {
			Result<Map<Object, Result<DataEntry>>> result = handler
					.getTairManager().prefixGets(namespace, pkey, skeyList);
			updateConfigVersion(handler);
			return result;
		}
		return new Result<Map<Object, Result<DataEntry>>>(
				ResultCode.CLIENTHANDLERERR);
	}

	public ResultCode prefixDelete(int namespace, Serializable pkey,
			Serializable skey) {
		ClusterHandler handler = handlerManager.pickHandler(skey, namespace);
		if (handler != null) {
			ResultCode result = handler.getTairManager().prefixDelete(
					namespace, pkey, skey);
			updateConfigVersion(handler);
			return result;
		}
		return ResultCode.CLIENTHANDLERERR;
	}

	public Result<Map<Object, ResultCode>> prefixDeletes(int namespace,
			Serializable pkey, List<? extends Serializable> skeys) {
		return new Result<Map<Object, ResultCode>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> prefixIncr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace,
			Serializable pkey, List<CounterPack> packList) {
		return new Result<Map<Object, Result<Integer>>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList) {
		return new Result<Map<Object, Result<Integer>>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode prefixSetCount(int namespace, Serializable pkey,
			Serializable skey, int count) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode prefixSetCount(int namespace, Serializable pkey,
			Serializable skey, int count, int version, int expireTime) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode prefixHide(int namespace, Serializable pkey,
			Serializable skey) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<DataEntry> prefixGetHidden(int namespace, Serializable pkey,
			Serializable skey) {
		return new Result<DataEntry>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, Result<DataEntry>>> prefixGetHiddens(
			int namespace, Serializable pkey, List<? extends Serializable> skeys) {
		return new Result<Map<Object, Result<DataEntry>>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, ResultCode>> prefixHides(int namespace,
			Serializable pkey, List<? extends Serializable> skeys) {
		return new Result<Map<Object, ResultCode>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode prefixInvalid(int namespace, Serializable pkey,
			Serializable skey, CallMode callMode) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode prefixHideByProxy(int namespace, Serializable pkey,
			Serializable skey, CallMode callMode) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<Map<Object, ResultCode>> prefixHidesByProxy(int namespace,
			Serializable pkey, List<? extends Serializable> skeys,
			CallMode callMode) {
		return new Result<Map<Object, ResultCode>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, ResultCode>> prefixInvalids(int namespace,
			Serializable pkey, List<? extends Serializable> skeys,
			CallMode callMode) {
		return new Result<Map<Object, ResultCode>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, Map<Object, Result<DataEntry>>>> mprefixGets(
			int namespace,
			Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap) {
		return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Map<Object, Map<Object, Result<DataEntry>>>> mprefixGetHiddens(
			int namespace,
			Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap) {
		return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode minvalid(int namespace, List<? extends Object> keys) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode mdelete(int namespace, List<? extends Object> keys) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBound, int upperBound) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBound, int upperBound) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode setCount(int namespace, Serializable key, int count) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode setCount(int namespace, Serializable key, int count,
			int version, int expireTime) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode addItems(int namespace, Serializable key,
			List<? extends Object> items, int maxCount, int version,
			int expireTime) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<DataEntry> getItems(int namespace, Serializable key,
			int offset, int count) {
		return new Result<DataEntry>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode removeItems(int namespace, Serializable key, int offset,
			int count) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<DataEntry> getAndRemove(int namespace, Serializable key,
			int offset, int count) {
		return new Result<DataEntry>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<Integer> getItemCount(int namespace, Serializable key) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public ResultCode lock(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public ResultCode unlock(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	public Result<List<Object>> mlock(int namespace, List<? extends Object> keys) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<List<Object>> mlock(int namespace,
			List<? extends Object> keys, Map<Object, ResultCode> failKeysMap) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<List<Object>> munlock(int namespace,
			List<? extends Object> keys) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Result<List<Object>> munlock(int namespace,
			List<? extends Object> keys, Map<Object, ResultCode> failKeysMap) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	public Map<String, String> getStat(int qtype, String groupName,
			long serverId) {
		return null;
	}

	public int getBucketOfKey(Serializable key) {
		return -1;
	}

	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

	public ClusterInfo getClusterInfo() {
		return clusterInfo;
	}

	public String getVersion() {
		return clientVersion;
	}

	public void setMaxFailCount(int failCount) {
		clusterInfo.setMaxFailCount(failCount);
	}

	public int getMaxFailCount() {
		return clusterInfo.getMaxFailCount();
	}

	public void setClusterShardStrategy(String clusterShardStrategy) {
		this.clusterShardStrategy = clusterShardStrategy;
	}

	public String getClusterShardStrategy() {
		return this.clusterShardStrategy;
	}

	// cmd to configserver
	public List<String> getNsStatus(List<String> groups) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.getNsStatus(groups);
	}

	public ResultCode setNsStatus(String group, int namespace, String status) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.setNsStatus(group, namespace, status);
	}

	public List<String> getGroupStatus(List<String> groups) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.getGroupStatus(groups);
	}

	public ResultCode setGroupStatus(String group, String status) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.setGroupStatus(group, status);
	}

	public List<String> getTmpDownServer(List<String> groups) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.getTmpDownServer(groups);
	}

	public ResultCode resetServer(String group, String dataServer) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.resetServer(group, dataServer);
	}

	// cmd to dataserver
	public ResultCode flushMmt(String group, String dataServer, int namespace) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.flushMmt(group, dataServer, namespace);
	}

	public ResultCode resetDb(String group, String dataServer, int namespace) {
		return infoUpdater.getMasterClusterHandler().getTairManager()
				.resetDb(group, dataServer, namespace);
	}

	public void setHeader(boolean flag) {
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			handler.getTairManager().setHeader(flag);
		}
	}

	public synchronized void setupLocalCache(int namespace) {
		setupLocalCache(namespace, 30, 30);
	}

	public synchronized void setupLocalCache(int namespace, int cap, long exp) {
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			handler.getTairManager().setupLocalCache(namespace, cap, exp);
		}
	}

	public synchronized void destroyLocalCache(int namespace) {
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			handler.getTairManager().destroyLocalCache(namespace);
		}
	}

	public synchronized void destroyAllLocalCache() {
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			handler.getTairManager().destroyAllLocalCache();
		}
	}

	private void updateConfigVersion(ClusterHandler handler) {
		int newVersion = handler.getConfigVersion();
		if (newVersion != handler.getLastVersion()) {
			handlerManager.update(true);
			handler.setLastVersion(newVersion);
		}
	}

	private void initClusterHandlerManager() {
		if (clusterShardStrategy.equalsIgnoreCase("bucket-hash")) {
			this.handlerManager = new HashBucketShardClusterHandlerManager(
					this.infoUpdater);
		} else if (clusterShardStrategy.equals("bucket-map")) {
			// this.handlerManager = new
			// MapBucketShardClusterHandlerManager(this.infoUpdater);
		} else {
			// default bucket-map
			this.handlerManager = new HashBucketShardClusterHandlerManager(
					this.infoUpdater);
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
		return new Result<Map<Object, ResultCode>>(
				ResultCode.TAIR_IS_NOT_SUPPORT);
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
		return null;
	}

	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBound, int upperBound) {
		return null;
	}

	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound) {
		return null;
	}

	public Result<Map<Object, ResultCode>> prefixSetCounts(int namespace,
			Serializable pkey, List<KeyCountPack> keyCountPacks) {
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

	public String getCharset() {
		return null;
	}

	public void setCharset(String charset) {
	}

	public int getCompressionThreshold() {
		return 0;
	}

	public void setCompressionThreshold(int compressionThreshold) {
	}

	public int getCompressionType() {
		return 0;
	}

	public void setCompressionType(int compressionType) {
	}

	public List<String> getConfigServerList() {
		return null;
	}

	public void setConfigServerList(List<String> configServerList) {
	}

	public void setDataServer(String dataServer) {
	}

	public String getGroupName() {
		return null;
	}

	public void setGroupName(String groupName) {
	}

	public int getMaxWaitThread() {
		return 0;
	}

	public int getTimeout() {
		return 0;
	}

	public void setTimeout(int timeout) {
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

	public Result<Map<Object, Result<DataEntry>>> simplePrefixGets(
			int namespace, Serializable pkey,
			List<? extends Serializable> subkeys) {
		return null;
	}

	public List<String> getIpOfKey(Object key) {
		List<String> ips = new ArrayList<String>();
		for (ClusterHandler handler : handlerManager.pickAllHandler()) {
			String ip = handler.getTairManager().getIpOfKey(key);
			if (ip != null) {
				ips.add(ip);
			}
		}
		return ips;
	}

	public String getTmpDownServer(String group) {
		return null;
	}

	public ResultCode queryGcStatus(int namespace) {
		return null;
	}

	public String getConfigId() {
		return null;
	}

	public boolean setFastdumpNamespaceGroupNum(int number) {
		return false;
	}

	public ResultCode initNamespace(int namespace, int count) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return ResultCode.NSERROR;
		}
		if (count < 1 || count > 10) {
			return ResultCode.INVALIDARG;
		}

		ResultCode rc = ResultCode.SUCCESS;
		// set_area_status on
    ArrayList<String> groups = handlerManager.getGroupNames();
		for (String group : groups) {
      log.warn("group:"+ group);
			rc = setNsStatus(group, namespace, "on");
			if (!rc.isSuccess()) {
				log.error("set group status failed " + rc);
				return rc;
			}
		}
		// wait for cs set status done
		try {
			Thread.sleep(2000);
		} catch (Exception e) {
		}

		List<String> params = new ArrayList<String>();
		List<String> retValues = new ArrayList<String>();
		// alloc_area_ring
		ClusterHandler[] handlers = handlerManager.pickAllHandler();
		if (handlers.length <= 0) {
			return ResultCode.SERVERERROR;
		}
		ClusterHandler handler = handlers[0];
		params.clear();
		retValues.clear();
		params.add(namespace + "");
		params.add(count + "");
		rc = handler
				.getTairManager()
				.opCmdToAdmin(
						TairConstant.ServerCmdType.TAIR_ADMIN_SERVER_CMD_ALLOC_AREA_RING
								.value(), params, retValues);
		if (rc.getCode() == ResultCode.SUCCESS.getCode()) {
			Iterator<String> iter = retValues.iterator();
			String msg = iter.next();
			log.info("alloc_area_ring success: " + namespace + " " + msg + " "
					+ handler.getTairManager().getGroupName());
		} else {
			String msg = retValues.get(0);
			if (msg != null) {
				log.error("alloc area ring failed " + rc + " message:" + msg
						+ " " + handler.getTairManager().getGroupName());
				if (msg.equals(new String("area ring was already initialized"))) {
					rc = ResultCode.NAMESPACE_EXISTED;
				}
			}
		}
		return rc;
	}
}
