/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.impl;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mina.common.IoSession;

import com.taobao.common.stable.CtSph;
import com.taobao.common.stable.Sph;
import com.taobao.common.stable.ValveType;
import com.taobao.eagleeye.EagleEye;
import com.taobao.middleware.logger.Level;
import com.taobao.monitor.MonitorLog;
import com.taobao.tair.CallMode;
import com.taobao.tair.CommandStatistic;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairCallback;
import com.taobao.tair.TairManager;
import com.taobao.tair.comm.CacheEntry;
import com.taobao.tair.comm.CacheEntry.Status;
import com.taobao.tair.comm.DataEntryLocalCache;
import com.taobao.tair.comm.DefaultTranscoder;
import com.taobao.tair.comm.LogGuardee;
import com.taobao.tair.comm.MultiSender;
import com.taobao.tair.comm.ResponseListener;
import com.taobao.tair.comm.TairClient;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.comm.TairClientFactory;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.CounterPack;
import com.taobao.tair.etc.IncData;
import com.taobao.tair.etc.KeyCountPack;
import com.taobao.tair.etc.KeyValuePack;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.TairAyncDecodeError;
import com.taobao.tair.etc.TairAyncInvokeTimeout;
import com.taobao.tair.etc.TairClientException;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairConstant.EngineType;
import com.taobao.tair.etc.TairConstant.MCOPS;
import com.taobao.tair.etc.TairOverflow;
import com.taobao.tair.etc.TairSendRequestStatus;
import com.taobao.tair.etc.TairTimeoutException;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.json.Json;
import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.MReturnPacket;
import com.taobao.tair.packet.PacketStreamer;
import com.taobao.tair.packet.RequestAddItemsPacket;
import com.taobao.tair.packet.RequestBulkWritePacket;
import com.taobao.tair.packet.RequestCommandCollection;
import com.taobao.tair.packet.RequestExpirePacket;
import com.taobao.tair.packet.RequestGetAndRemoveItemsPacket;
import com.taobao.tair.packet.RequestGetExpirePacket;
import com.taobao.tair.packet.RequestGetHiddenPacket;
import com.taobao.tair.packet.RequestGetItemsCountPacket;
import com.taobao.tair.packet.RequestGetItemsPacket;
import com.taobao.tair.packet.RequestGetModifyDatePacket;
import com.taobao.tair.packet.RequestGetPacket;
import com.taobao.tair.packet.RequestGetRangePacket;
import com.taobao.tair.packet.RequestHideByProxyPacket;
import com.taobao.tair.packet.RequestHidePacket;
import com.taobao.tair.packet.RequestIncDecBoundedPacket;
import com.taobao.tair.packet.RequestIncDecPacket;
import com.taobao.tair.packet.RequestInvalidPacket;
import com.taobao.tair.packet.RequestLazyRemoveAreaPacket;
import com.taobao.tair.packet.RequestLockPacket;
import com.taobao.tair.packet.RequestMPutPacket;
import com.taobao.tair.packet.RequestBulkWriteV2Packet;
import com.taobao.tair.packet.RequestMcOpsPacket;
import com.taobao.tair.packet.RequestOpCmdPacket;
import com.taobao.tair.packet.RequestPrefixGetHiddensPacket;
import com.taobao.tair.packet.RequestPrefixGetsPacket;
import com.taobao.tair.packet.RequestPrefixHidesByProxyPacket;
import com.taobao.tair.packet.RequestPrefixHidesPacket;
import com.taobao.tair.packet.RequestPrefixIncDecBoundedPacket;
import com.taobao.tair.packet.RequestPrefixIncDecPacket;
import com.taobao.tair.packet.RequestPrefixInvalidsPacket;
import com.taobao.tair.packet.RequestPrefixPutsPacket;
import com.taobao.tair.packet.RequestPrefixRemovesPacket;
import com.taobao.tair.packet.RequestPutModifyDatePacket;
import com.taobao.tair.packet.RequestPutPacket;
import com.taobao.tair.packet.RequestQueryBulkWriteTokenPacket;
import com.taobao.tair.packet.RequestQueryGcStatusPacket;
import com.taobao.tair.packet.RequestRemoveArea;
import com.taobao.tair.packet.RequestRemoveItemsPacket;
import com.taobao.tair.packet.RequestRemovePacket;
import com.taobao.tair.packet.RequestSimplePrefixGetsPacket;
import com.taobao.tair.packet.RequestStatisticsPacket;
import com.taobao.tair.packet.ResponseBulkWritePacket;
import com.taobao.tair.packet.ResponseGetExpirePacket;
import com.taobao.tair.packet.ResponseGetItemsPacket;
import com.taobao.tair.packet.ResponseGetModifyDatePacket;
import com.taobao.tair.packet.ResponseGetPacket;
import com.taobao.tair.packet.ResponseGetRangePacket;
import com.taobao.tair.packet.ResponseIncDecBoundedPacket;
import com.taobao.tair.packet.ResponseIncDecPacket;
import com.taobao.tair.packet.ResponseMcOpsPacket;
import com.taobao.tair.packet.ResponseOpCmdPacket;
import com.taobao.tair.packet.ResponsePrefixGetsPacket;
import com.taobao.tair.packet.ResponsePrefixIncDecBoundedPacket;
import com.taobao.tair.packet.ResponsePrefixIncDecPacket;
import com.taobao.tair.packet.ResponseBulkWriteV2Packet;
import com.taobao.tair.packet.ResponseQueryBulkWriteTokenPacket;
import com.taobao.tair.packet.ResponseQueryGcStatusPacket;
import com.taobao.tair.packet.ResponseSimplePrefixGetsPacket;
import com.taobao.tair.packet.ResponseStatisticsPacket;
import com.taobao.tair.packet.ReturnPacket;
import com.taobao.tair.packet.TairPacketStreamer;

public class DefaultTairManager implements TairManager {
	private static com.taobao.middleware.logger.Logger logger = com.taobao.middleware.logger.LoggerFactory
			.getLogger("com.taobao.tair");
	static {
		logger.setLevel(Level.WARN);
		logger.activateAppender("tair-client", "tair-client.log", "UTF-8");
		logger.setAdditivity(false);
	}
	private static final Logger log = LoggerFactory
			.getLogger(DefaultTairManager.class);
	protected static String clientVersion = "TairClient 2.3.4";
	protected static int defaultServerPort = 5191;
	protected List<String> configServerList = null;
	protected String groupName = null;
	protected boolean forceService = false;
	protected ConfigServer configServer = null;
	protected InvalidServerManager invalidServerManager = null;
	protected AdminServerManager adminServerManager = null;
	protected long serverId = 0;
	protected String dataServer = null;
	protected Boolean isDirect = false;
	protected MultiSender multiSender = null;
	protected int timeout = TairConstant.DEFAULT_TIMEOUT;
	protected int asyncTimeout = TairConstant.DEFAULT_TIMEOUT;
	protected int maxWaitThread = TairConstant.DEFAULT_WAIT_THREAD;
	protected TairPacketStreamer packetStreamer = null;
	protected Transcoder transcoder = null;
	protected int compressionThreshold = 0;
	protected int compressionType = TairConstant.TAIR_DEFAULT_COMPRESSION_TYPE;
	protected String charset = null;
	protected String name = null;
	protected int maxFailCount = 100;
	protected AtomicInteger failCounter = new AtomicInteger(0);
	protected Sph threadCount = null;
	protected boolean checkDownNodes = false;
	protected boolean sharedClientFactory = true;
	protected boolean header = true;
	protected boolean monitorMode = false;
	protected int logStep = 100;
	protected LogGuardee logGuardee = new LogGuardee(logStep);
	protected boolean supportBackupMode = false;
	protected int fastdumpNamespaceGroupNumer = 4;
	static int fastdumpReservedNamespace = 10;
	private ConfigServerUpdater csUpdater;
	private int namespaceOffset = 0;
	private boolean inited = false;
	private ConcurrentHashMap<Integer, CommandStatistic> cstatMap = null;
	private String invalidServiceDomain = null;
	private String invalidServiceCluster = null;
	private static final int TESTFLOW_PERM_TAIR_EXPIRED_TIME = 10 * 24 * 3600;
	// to store stat header, so only need fetch stat header at first run
	// stat schema version --> StatisticsAnalyser, stat header cache
	private Map<Long, StatisticsAnalyser> statAnalyserCache = new HashMap<Long, StatisticsAnalyser>();

	public void setLogLevel(Level logLevel) {
		logger.setLevel(logLevel);
	}

	public int getNamespaceOffset() {
		return namespaceOffset;
	}

	public void setNamespaceOffset(int namespaceOffset) {
		if ((namespaceOffset < 0)
				|| (namespaceOffset > TairConstant.NAMESPACE_MAX)) {
			throw new RuntimeException(clientVersion
					+ ": setNamespaceOffset failed, namespaceOffset: "
					+ namespaceOffset);
		}
		this.namespaceOffset = namespaceOffset;
	}

	// use for identify each storage engine
	protected EngineType engineType = EngineType.COMMON;

	protected TairClientFactory clientFactory = null;

	protected ClassLoader customClassLoader = DefaultTairManager.class
			.getClassLoader();

	protected ConcurrentHashMap<Integer, DataEntryLocalCache> localCacheMap = new ConcurrentHashMap<Integer, DataEntryLocalCache>();

	public DefaultTairManager() {
		this("DefaultTairManager", true, Runtime.getRuntime()
				.availableProcessors() + 1);
	}

	public DefaultTairManager(String name, boolean sharedFactory,
			int processorCount) {
		this.name = name;
		this.namespaceOffset = 0;
		this.sharedClientFactory = sharedFactory;
		if (sharedFactory)
			clientFactory = TairClientFactory.getSingleInstance();
		else
			clientFactory = new TairClientFactory(processorCount);
	}

	public DefaultTairManager(String name, boolean sharedFactory) {
		this(name, sharedFactory,
				Runtime.getRuntime().availableProcessors() + 1);
	}

	public void setSendBufferSize(int size) {
		clientFactory.setSendBufferSize(size);
	}

	public synchronized void setupLocalCache(int namespace) {
		setupLocalCache(namespace, 30, 30);
	}

	public synchronized void setupLocalCache(int namespace, int cap, long exp,
			ClassLoader cl) {
		namespace += this.getNamespaceOffset();
		if (!localCacheMap.containsKey(namespace)) {
			String uuid = UUID.randomUUID().toString();
			DataEntryLocalCache localCache = new DataEntryLocalCache(namespace
					+ " " + uuid, cl);
			localCache.setCapacity(cap);
			localCache.setExpireTime(exp);
			if (localCacheMap.putIfAbsent(namespace, localCache) != null)
				localCache.destroy();
		}
	}

	public synchronized void setupLocalCache(int namespace, int cap, long exp) {
		setupLocalCache(namespace, cap, exp, customClassLoader);
	}

	public synchronized void enhanceLocalCache(int namespace) {
		if (!localCacheMap.containsKey(namespace)) {
			setupLocalCache(namespace - getNamespaceOffset()); // use the
																// default setup
		} else {
			DataEntryLocalCache cache = getLocalCache(namespace
					- getNamespaceOffset());
			cache.enhance();
		}
	}

	public DataEntryLocalCache getLocalCache(int namespace) {
		namespace += this.getNamespaceOffset();
		return localCacheMap.get(namespace);
	}

	public final Map<Integer, DataEntryLocalCache> getAllLocalCache() {
		return this.localCacheMap;
	}

	public synchronized void destroyLocalCache(int namespace) {
		namespace += this.getNamespaceOffset();
		DataEntryLocalCache localCache = localCacheMap.get(namespace);
		localCacheMap.remove(namespace);
		if (localCache != null) {
			localCache.destroy();
		}
	}

	public synchronized void destroyAllLocalCache() {
		for (DataEntryLocalCache cache : localCacheMap.values()) {
			cache.destroy();
		}
		localCacheMap.clear();
	}

	public void setSupportBackupMode(boolean supportBackupMode) {
		this.supportBackupMode = supportBackupMode;
		if (configServer != null) {
			configServer.setSupportBackupMode(supportBackupMode);
		}
	}

	public void setSessionIdleTime(int idleTime) {
		this.clientFactory.setSessionIdleTime(idleTime);
	}

	/**
	 * force to update configure server
	 */
	protected void updateConfigServer() {
		this.checkConfigVersion(0);
		failCounter.set(0);
	}

	public void init() {
		try {
			initInternal();
		} catch (RuntimeException e) {
			this.close();
			throw e;
		}
	}

	private void initInternal() {
		transcoder = new DefaultTranscoder(compressionThreshold, charset,
				this.compressionType);
		((DefaultTranscoder) transcoder).setWithHeader(header);
		((DefaultTranscoder) transcoder).setMonitorMode(monitorMode);
		((DefaultTranscoder) transcoder)
				.setCustomClassLoader(customClassLoader);
		packetStreamer = new TairPacketStreamer(transcoder, engineType);
		invalidServerManager = new InvalidServerManager(clientFactory,
				packetStreamer, invalidServiceDomain, invalidServiceCluster);
		if (!invalidServerManager.init()) {
			throw new RuntimeException(
					clientVersion
							+ ": init invalid server manager failed, invalid service domain: "
							+ invalidServiceDomain + ", cluster :"
							+ invalidServiceCluster);
		}

		adminServerManager = new AdminServerManager(clientFactory,
				packetStreamer);

		if (!this.isDirect) {
			configServer = new ConfigServer(clientFactory, groupName,
					configServerList, packetStreamer, invalidServerManager,
					adminServerManager);

			configServer.setForceService(forceService);
			configServer.setCheckDownNodes(checkDownNodes);
			configServer.setSupportBackupMode(supportBackupMode);
			if (!configServer.retrieveConfigure()) {
				throw new RuntimeException(clientVersion
						+ ": init config failed, group name: " + groupName);
			}
			csUpdater = new ConfigServerUpdater(configServer);
			csUpdater.setName("CsUpdater" + groupName);
			csUpdater.start();
		}

		multiSender = new MultiSender(clientFactory, packetStreamer);

		if (threadCount != null) {
			log.error("Sph has bean initialized! init sph failed, group name: "
					+ groupName);
		} else {
			// 500 of the maximum thread count, 1000ms of the timeout threshold
			threadCount = new CtSph("tair-"
					+ (this.isDirect ? "direct-connect-ds" : groupName), 500,
					1000, ValveType.COUNT_AND_AVGELAPSED_VALVE_TYPE);
			if (maxWaitThread > 0) {
				setMaxWaitThread(maxWaitThread);
			}
		}

		log.warn(name + " [" + getVersion() + "] started...");
		this.inited = true;
	}

	private static boolean isTestFlow() {
		return "1".equals(EagleEye.getUserData("t"));
	}

	public void setHeader(boolean flag) {
		header = flag;
		if (this.transcoder instanceof DefaultTranscoder) {
			((DefaultTranscoder) this.transcoder).setWithHeader(flag);
		}
	}

	public void setMonitorMode(boolean flag) {
		monitorMode = flag;
	}

	public void setMaxWaitThread(int maxThreadCount) {
		if (threadCount != null && threadCount instanceof CtSph) {
			CtSph threadCountRef = (CtSph) threadCount;
			threadCountRef.setDefaultCountValve(maxThreadCount);
		}
		this.maxWaitThread = maxThreadCount;
	}

	public ClassLoader getCustomClassLoader() {
		return customClassLoader;
	}

	public void setCustomClassLoader(ClassLoader customClassLoader) {
		this.customClassLoader = customClassLoader;
		if (this.transcoder != null) {
			if (this.transcoder instanceof DefaultTranscoder) {
				((DefaultTranscoder) this.transcoder)
						.setCustomClassLoader(customClassLoader);
			}
		}
		if (localCacheMap != null) {
			setLocalCacheCustumClassLoader(customClassLoader);
		}
	}

	private synchronized void setLocalCacheCustumClassLoader(ClassLoader cl) {

		for (Map.Entry<Integer, DataEntryLocalCache> entry : localCacheMap
				.entrySet()) {
			DataEntryLocalCache localCache = entry.getValue();
			if (localCache != null) {
				localCache.setCustomClassLoader(cl);
			}
		}
	}

	public Map<Long, Set<Serializable>> classifyKeys(
			Collection<? extends Serializable> keys)
			throws IllegalArgumentException {
		Map<Long, Set<Serializable>> buckets = new HashMap<Long, Set<Serializable>>();
		for (Serializable key : keys) {
			long code = this.serverId;

			if (!this.isDirect) {
				code = configServer.getServer(transcoder.encode(key), true);
			}

			if (buckets.containsKey(code)) {
				Set<Serializable> bucket = buckets.get(code);
				bucket.add(key);
			} else {
				Set<Serializable> bucket = new HashSet<Serializable>();
				bucket.add(key);
				buckets.put(code, bucket);
			}
		}
		return buckets;
	}

	private TairClient getClient(Long address) {
		if (TairUtil.mockMode) {
			return null;
		}

		String host = TairUtil.idToAddress(address);
		if (host != null) {
			try {
				return createClient(host, timeout, timeout, packetStreamer);
			} catch (TairClientException e) {
				log.error("getClient failed " + host, e);
			}
		}
		return null;
	}

	private TairClient getClient(int bucket, boolean isRead) {
		long address = configServer.getServer(bucket, isRead);
		return getClient(address);
	}

	private TairClient getClient(Object key, boolean isRead) {
		long address = this.serverId;

		if (!this.isDirect) {
			address = configServer.getServer(transcoder.encode(key, true),
					isRead);
		}
		return getClient(address);
	}

	protected BasePacket sendRequest(int namesapce, Object key,
			BasePacket packet, TairSendRequestStatus status) {
		return sendRequest(namesapce, key, packet, false, status);
	}

	public String getIpOfKey(Object key) {
		return TairUtil.idToAddress(configServer.getServer(
				transcoder.encode(key, true), true));
	}

	private class AsyncCallListener implements ResponseListener {
		protected AsyncCallListener(TairCallback cb) {
			this.cb = cb;
		}

		private TairCallback cb;

		public void responseReceived(Object response) {
			if (response == null) {
				final String msg = "Response is NULL";
				log.error(msg);
				if (cb != null)
					cb.callback(new TairAyncDecodeError(msg));

			} else if (!(response instanceof BasePacket)) {
				final String msg = response.getClass().toString()
						+ " is not BasePacket";
				log.error(msg);
				if (cb != null)
					cb.callback(new TairAyncDecodeError(msg));

			} else {
				BasePacket r = null;
				r = (BasePacket) response;
				r.decode();
				csUpdater.check(r.getConfigVersion());
				if (cb != null)
					cb.callback(r);
			}
		}

		public void exceptionCaught(IoSession session,
				TairClientException exception) {
			if (!(exception instanceof TairAyncInvokeTimeout)) {
				log.error("do async request failed", exception);
				if (session.isConnected()) {
					log.error("session closing", exception);
					session.close();
				}
			}
			cb.callback(exception);
		}
	}

	private ResultCode sendAsyncRequest(int ns, TairClient client,
			BasePacket packet, TairCallback cb, SERVER_TYPE type, TairSendRequestStatus status) {
		if (client == null) {
			int value = failCounter.incrementAndGet();
			if (value > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}
			log.warn("conn is null ");
			return ResultCode.CONNERROR;
		}

		if (!client.invokeAsync(ns, packet, asyncTimeout,
				new AsyncCallListener(cb), type, status)) {
			if (null != status && status.isFlowControl()) {
				return ResultCode.TAIR_RPC_OVERFLOW;
			}
			return ResultCode.ASYNCERR;
		}

		return ResultCode.SUCCESS;
	}

	protected ResultCode sendAsyncRequest(int ns, Long serverIp,
			BasePacket packet, boolean isRead, TairCallback cb, SERVER_TYPE type, TairSendRequestStatus status) {
		TairClient client = getClient(serverIp);
		return sendAsyncRequest(ns, client, packet, cb, type, status);
	}

	protected ResultCode sendAsyncRequest(int ns, Object key, BasePacket packet,
			boolean isRead, TairCallback cb, SERVER_TYPE type, TairSendRequestStatus status) {
		TairClient client = getClient(key, isRead);
		return sendAsyncRequest(ns, client, packet, cb, type, status);
	}

	protected BasePacket sendRequest(int namespace, Object key,
			BasePacket packet, boolean isRead, TairSendRequestStatus status) {
		TairClient client = getClient(key, isRead);
		return sendRequest(namespace, client, packet, status);
	}

	protected BasePacket sendRequest(int namespace, Long serverIp,
			BasePacket packet, TairSendRequestStatus status) {
		TairClient client = getClient(serverIp);
		return sendRequest(namespace, client, packet, status);
	}

	protected BasePacket sendRequest(int namespace, int bucket,
			BasePacket packet, TairSendRequestStatus status) {
		TairClient client = getClient(bucket, false); // get server form bucket
														// for bulkwrite
		return sendRequest(namespace, client, packet, status);
	}

	protected BasePacket sendRequest(int namespace, TairClient client,
			BasePacket packet, TairSendRequestStatus status) {
		if (client == null) {
			int value = failCounter.incrementAndGet();

			if (value > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}

			log.warn("conn is null ");
			return null;
		}

		BasePacket returnPacket = null;
		long startTime = System.currentTimeMillis();

		try {
			if (threadCount.entry()) {
				try {
					returnPacket = (BasePacket) client.invoke(namespace,
							packet, timeout, getGroupName());
				} catch (TairClientException e) {
					// handle the Timeout Exception by logGuardee
					if (e instanceof TairTimeoutException) {
						if (logGuardee.guardException((TairTimeoutException) e)) {
							log.error("send request to " + client + " failed ",
									e);
						}
					} else {
						if (e instanceof TairOverflow && null != status) {
							status.setFlowControl(true);
						}
						log.error("send request to " + client + " failed ", e);
					}
				}
				long endTime = System.currentTimeMillis();

				if (returnPacket == null) {
					if (failCounter.incrementAndGet() > maxFailCount) {
						this.checkConfigVersion(0);
						failCounter.set(0);
						log.warn("connection failed happened 100 times, sync configuration");
					}
					return null;
				} else {
					if (log.isDebugEnabled()) {
						log.debug("timeout: " + timeout + ", used: "
								+ (endTime - startTime) + " (ms), client: "
								+ client);
					}
				}
			} else {
				log.warn("the tair request was limited by SPH, slow down.");
				return null;
			}
		} finally {
			threadCount.release();
		}

		return returnPacket;
	}

	public Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime) {
		namespace += this.getNamespaceOffset();
		if (value < 0) {
			MonitorLog.addStat(clientVersion, "decr/error/ITEMSIZEERROR", null);
			return new Result<Integer>(ResultCode.ITEMSIZEERROR);
		}

		return addCount(namespace, key, -value, defaultValue, expireTime);
	}

	public Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBounded, int upperBounded) {
		namespace += this.getNamespaceOffset();
		return addCount(namespace, key, -value, defaultValue, expireTime,
				lowBounded, upperBounded);
	}

	public ResultCode delete(int namespace, Serializable key) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "delete/error/NSERROR", null);
			return ResultCode.NSERROR;
		}

		if (key == null) {
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode resultCode = ResultCode.CONNERROR;
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(key);
			MonitorLog.addStat(clientVersion, "delete/localcache", null);
		}

		long s = System.currentTimeMillis();
		RequestRemovePacket packet = new RequestRemovePacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "delete/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "delete/error/SERIALIZEERROR",
					null);
			return ResultCode.SERIALIZEERROR;
		}

		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			resultCode = ResultCode.valueOf(((ReturnPacket) returnPacket)
					.getCode());
			this.checkConfigVersion(returnPacket.getConfigVersion());
		} else {
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "delete/exception", null);
		}
		long e = System.currentTimeMillis();

		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "delete", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog.addStat(clientVersion, "delete", null, (e - s), 1);
		}

		return resultCode;
	}

	public ResultCode hide(int namespace, Serializable key) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "hide/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(key);
			MonitorLog.addStat(clientVersion, "/hide/delete/localcache", null);
		}

		long s = System.currentTimeMillis();
		RequestHidePacket packet = new RequestHidePacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);

		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "hide/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 3) {
			MonitorLog
					.addStat(clientVersion, "hide/error/SERIALIZEERROR", null);
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			rc = ResultCode.valueOf(((ReturnPacket) returnPacket).getCode());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "hide/exception", null);
		}
		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "hide", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog.addStat(clientVersion, "hide", null, (e - s), 1);
		}
		return rc;
	}

	/*
	 * SUCCESS, or INVAL_CONN_ERROR
	 * 
	 * @NOTE: if no invalid server available, delete is used.
	 */
	public ResultCode invalid(int namespace, Serializable key) {
		return invalid(namespace, key, CallMode.SYNC);
	}

	private TairClient createClient(final String targetUrl,
			final int connectionTimeout, final int waitTimeout,
			final PacketStreamer pstreamer) throws TairClientException {
		TairClient client = null;
		if ((configServer != null) && (configServer.isAllDead()))
			client = clientFactory.get(targetUrl, connectionTimeout, 0,
					pstreamer);
		else
			client = clientFactory.get(targetUrl, connectionTimeout,
					waitTimeout, pstreamer);
		client.setTairManager(this);
		return client;
	}

	/*
	 * @RETURN: SUCCESS, INVAL_CONN_ERROR, or QUEUE_OVERFLOWED when
	 * ASYNC_INVALID
	 * 
	 * @NOTE: if no invalid server available, delete is used.
	 */
	public ResultCode invalid(int namespace, Serializable key, CallMode callMode) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		InvalidServer invalidServer = invalidServerManager
				.chooseInvalidServer();
		if (invalidServer == null) {
			log.debug("no invalid server available, use delete instead");
			return delete(namespace - this.getNamespaceOffset(), key);
		}
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "invalid/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		log.debug("send invalid request to " + invalidServer.getAddress());
		long s = System.currentTimeMillis();
		RequestInvalidPacket packet = new RequestInvalidPacket(transcoder,
				groupName);
		packet.setNamespace((short) namespace);

		if (callMode == CallMode.SYNC) {
			packet.setSync(0);
		} else if (callMode == CallMode.ASYNC) {
			packet.setSync(1);
		} else {
			throw new UnsupportedOperationException("can't reach here");
		}

		packet.addKey(key);

		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "invalid/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "invalid/error/SERIALIZEERROR",
					null);
			return ResultCode.SERIALIZEERROR;
		}

		BasePacket returnPacket = null;
		ResultCode rc = ResultCode.CONNERROR;

		do {
			invalidLocalCache(namespace, key);
			TairClient client = null;
			try {
				client = createClient(invalidServer.getAddress(), timeout,
						timeout, packetStreamer);
				if (client != null) {
					returnPacket = (BasePacket) client.invoke(namespace,
							packet, timeout, getGroupName());
				} else {
					log.error("connect to " + invalidServer.getAddress()
							+ " failed");
					int value = failCounter.addAndGet(1);
					if (value > maxFailCount) {
						this.checkConfigVersion(0);
						failCounter.set(0);
					}
				}
			} catch (Exception e) {
				// handle the Timeout Exception by logGuardee
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + client + " failed ", e);
					}
				} else {
					log.error("exception when send packet to "
							+ invalidServer.getAddress() + e);
				}
				int value = failCounter.addAndGet(1);
				if (value > maxFailCount) {
					this.checkConfigVersion(0);
					failCounter.set(0);
				}
			}

			if (returnPacket == null) {
				invalidServer.failed();
				break;
			}
			if (returnPacket instanceof ReturnPacket) {
				rc = ResultCode
						.valueOf(((ReturnPacket) returnPacket).getCode());
				if (rc.equals(ResultCode.QUEUE_OVERFLOWED)) {
					log.warn("async queue of invalid server overflowed, using delete");
					return delete(namespace - this.getNamespaceOffset(), key);
				}
				if (rc.equals(ResultCode.SUCCESS)) {
					invalidServer.successed();
				} else {
					invalidServer.failed();
					MonitorLog
							.addStat(clientVersion, "invalid/exception", null);
				}
			}
		} while (false);

		long e = System.currentTimeMillis();

		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "invalid", returnPacket
					.getRemoteAddress().toString() + "$" + namespace, (e - s),
					1);
		} else {
			MonitorLog.addStat(clientVersion, "invalid", null, (e - s), 1);
		}

		return rc;
	}

	public ResultCode hideByProxy(int namespace, Serializable key) {
		return hideByProxy(namespace, key, CallMode.SYNC);
	}

	public ResultCode hideByProxy(int namespace, Serializable key,
			CallMode callMode) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		InvalidServer invalidServer = invalidServerManager
				.chooseInvalidServer();
		if (invalidServer == null) {
			log.debug("no invalid server available, use hide instead");
			return hide(namespace - this.getNamespaceOffset(), key);
		}
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog
					.addStat(clientVersion, "hideByProxy/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		log.debug("send hideByProxy request to " + invalidServer.getAddress());
		long s = System.currentTimeMillis();
		RequestHideByProxyPacket packet = new RequestHideByProxyPacket(
				transcoder, groupName);
		if (callMode == CallMode.SYNC) {
			packet.setSync(0);
		} else if (callMode == CallMode.ASYNC) {
			packet.setSync(1);
		} else {
			throw new UnsupportedOperationException();
		}

		packet.setNamespace((short) namespace);
		packet.addKey(key);

		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "hideByProxy/error/KEYTOLARGE",
					null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"hideByProxy/error/SERIALIZEERROR", null);
			return ResultCode.SERIALIZEERROR;
		}

		BasePacket returnPacket = null;
		ResultCode rc = ResultCode.CONNERROR;

		do {
			TairClient client = null;
			try {
				client = createClient(invalidServer.getAddress(), timeout,
						timeout, packetStreamer);
				if (client != null) {
					returnPacket = (BasePacket) client.invoke(namespace,
							packet, timeout, getGroupName());
				} else {
					log.error("connect to " + invalidServer.getAddress()
							+ " failed");
					int value = failCounter.addAndGet(1);
					if (value > maxFailCount) {
						this.checkConfigVersion(0);
						failCounter.set(0);
					}
				}
			} catch (Exception e) {
				// handle the Timeout Exception by logGuardee
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + client + " failed ", e);
					}
				} else {
					log.error("exception when send packet to "
							+ invalidServer.getAddress() + e);
				}
				int value = failCounter.addAndGet(1);
				if (value > maxFailCount) {
					this.checkConfigVersion(0);
					failCounter.set(0);
				}
			}

			if (returnPacket == null) {
				invalidServer.failed();
				break;
			}
			if (returnPacket instanceof ReturnPacket) {
				rc = ResultCode
						.valueOf(((ReturnPacket) returnPacket).getCode());
				if (rc.equals(ResultCode.QUEUE_OVERFLOWED)) {
					log.warn("async queue of invalid server overflowed, using hide");
					return hide(namespace - this.getNamespaceOffset(), key);
				}
				if (rc.equals(ResultCode.SUCCESS)) {
					invalidServer.successed();
				} else {
					invalidServer.failed();
					MonitorLog.addStat(clientVersion, "hideByProxy/exception",
							null);
				}
			}
		} while (false);

		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "hideByProxy", returnPacket
					.getRemoteAddress().toString() + "$" + namespace, (e - s),
					1);
		} else {
			MonitorLog.addStat(clientVersion, "hideByProxy", null, (e - s), 1);
		}
		return rc;
	}

	public ResultCode prefixInvalid(int namespace, Serializable pkey,
			Serializable skey, CallMode callMode) {
		// Needn't add namespace offset
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return invalid(namespace, key, callMode);
	}

	public ResultCode prefixHideByProxy(int namespace, Serializable pkey,
			Serializable skey, CallMode callMode) {
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		namespace += this.getNamespaceOffset();
		return hideByProxy(namespace - this.getNamespaceOffset(), key, callMode);
	}

	public Result<Map<Object, ResultCode>> prefixHidesByProxy(int namespace,
			Serializable pkey, List<? extends Serializable> skeys,
			CallMode callMode) {
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		InvalidServer invalidServer = invalidServerManager
				.chooseInvalidServer();
		if (invalidServer == null) {
			log.debug("no invalid server available, use prefixHides instead");
			return prefixHides(namespace - this.getNamespaceOffset(), pkey,
					skeys);
		}
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			MonitorLog.addStat(clientVersion,
					"prefixHidesByProxy/error/NSERROR", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}
		if (skeys.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, ResultCode>>(ResultCode.TOO_MANY_KEYS);
		}

		RequestPrefixHidesByProxyPacket packet = new RequestPrefixHidesByProxyPacket(
				transcoder, groupName);
		packet.setNamespace((short) namespace);
		if (callMode == CallMode.SYNC) {
			packet.setSync(0);
		} else if (callMode == CallMode.ASYNC) {
			packet.setSync(1);
		} else {
			throw new UnsupportedOperationException("unknown call mode");
		}
		for (Serializable skey : skeys) {
			MixedKey key = new MixedKey(transcoder, pkey, skey);
			packet.addKey(key);
		}

		long s = System.currentTimeMillis();
		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"prefixHidesByProxy/error/KEYTOLARGE", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixHidesByProxy/error/SERIALIZEERROR", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}
		BasePacket returnPacket = null;
		ResultCode rc = ResultCode.CONNERROR;
		do {
			TairClient client = null;
			try {
				client = createClient(invalidServer.getAddress(), timeout,
						timeout, packetStreamer);
				if (client != null) {
					returnPacket = (BasePacket) client.invoke(namespace,
							packet, timeout, getGroupName());
				} else {
					log.error("connect to " + invalidServer.getAddress()
							+ " failed");
					int value = failCounter.addAndGet(1);
					if (value > maxFailCount) {
						this.checkConfigVersion(0);
						failCounter.set(0);
					}
				}
			} catch (Exception e) {
				// handle the Timeout Exception by logGuardee
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + client + " failed ", e);
					}
				} else {
					log.error("exception when send packet to "
							+ invalidServer.getAddress() + e);
				}
				int value = failCounter.addAndGet(1);
				if (value > maxFailCount) {
					this.checkConfigVersion(0);
					failCounter.set(0);
				}
			}
			if (returnPacket == null) {
				invalidServer.failed();
				break;
			}
			if (returnPacket instanceof ReturnPacket) {
				rc = ResultCode
						.valueOf(((ReturnPacket) returnPacket).getCode());
				if (rc.equals(ResultCode.QUEUE_OVERFLOWED)) {
					log.warn("async queue of invalid server overflowed, using prefixHides");
					return prefixHides(namespace - this.getNamespaceOffset(),
							pkey, skeys);
				}
				if (rc.equals(ResultCode.SUCCESS)) {
					invalidServer.successed();
				} else {
					invalidServer.failed();
					MonitorLog.addStat(clientVersion,
							"prefixHidesByProxy/exception", null);
				}
			}
		} while (false);

		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixHidesByProxy",
					returnPacket.getRemoteAddress().toString() + "$"
							+ namespace, e - s, 1);
		} else {
			MonitorLog.addStat(clientVersion, "prefixHidesByProxy", null,
					e - s, 1);
		}

		return new Result<Map<Object, ResultCode>>(rc);
	}

	public Result<Map<Object, ResultCode>> prefixInvalids(int namespace,
			Serializable pkey, List<? extends Serializable> skeys,
			CallMode callMode) {
		InvalidServer invalidServer = invalidServerManager
				.chooseInvalidServer();
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if (invalidServer == null) {
			log.debug("no invalid server available, use prefixDeletes instead");
			return prefixDeletes(namespace - this.getNamespaceOffset(), pkey,
					skeys);
		}
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			MonitorLog.addStat(clientVersion, "prefixInvalids/error/NSERROR",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}
		if (skeys.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, ResultCode>>(ResultCode.TOO_MANY_KEYS);
		}

		RequestPrefixInvalidsPacket packet = new RequestPrefixInvalidsPacket(
				transcoder, groupName);
		packet.setNamespace((short) namespace);
		if (callMode == CallMode.SYNC) {
			packet.setSync(0);
		} else if (callMode == CallMode.ASYNC) {
			packet.setSync(1);
		} else {
			throw new UnsupportedOperationException("unknown call mode");
		}
		for (Serializable skey : skeys) {
			MixedKey key = new MixedKey(transcoder, pkey, skey);
			invalidLocalCache(namespace, key);
			packet.addKey(key);
		}

		long s = System.currentTimeMillis();
		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"prefixInvalids/error/KEYTOLARGE", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixInvalids/error/SERIALIZEERROR", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}
		BasePacket returnPacket = null;
		ResultCode rc = ResultCode.CONNERROR;
		do {
			TairClient client = null;
			try {
				client = createClient(invalidServer.getAddress(), timeout,
						timeout, packetStreamer);
				if (client != null) {
					returnPacket = (BasePacket) client.invoke(namespace,
							packet, timeout, getGroupName());
				} else {
					log.error("connect to " + invalidServer.getAddress()
							+ " failed");
					int value = failCounter.addAndGet(1);
					if (value > maxFailCount) {
						this.checkConfigVersion(0);
						failCounter.set(0);
					}
				}
			} catch (Exception e) {
				// handle the Timeout Exception by logGuardee
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + client + " failed ", e);
					}
				} else {
					log.error("exception when send packet to "
							+ invalidServer.getAddress() + e);
				}
				int value = failCounter.addAndGet(1);
				if (value > maxFailCount) {
					this.checkConfigVersion(0);
					failCounter.set(0);
				}
			}
			if (returnPacket == null) {
				invalidServer.failed();
				break;
			}
			if (returnPacket instanceof ReturnPacket) {
				rc = ResultCode
						.valueOf(((ReturnPacket) returnPacket).getCode());
				if (rc.equals(ResultCode.QUEUE_OVERFLOWED)) {
					log.warn("async queue of invalid server overflowed, using prefixDeletes");
					return prefixDeletes(namespace - this.getNamespaceOffset(),
							pkey, skeys);
				}
				if (rc.equals(ResultCode.SUCCESS)) {
					invalidServer.successed();
				} else {
					invalidServer.failed();
					MonitorLog.addStat(clientVersion,
							"prefixInvalids/exception", null);
				}
			}
		} while (false);

		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixInvalids", returnPacket
					.getRemoteAddress().toString() + "$" + namespace, e - s, 1);
		} else {
			MonitorLog.addStat(clientVersion, "prefixInvalids", null, e - s, 1);
		}
		return new Result<Map<Object, ResultCode>>(rc);
	}

	public Result<Map<Object, Map<Object, Result<DataEntry>>>> mprefixGets(
			int namespace,
			Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap) {
		if (!this.inited) {
			return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			MonitorLog
					.addStat(clientVersion, "mprefixGets/error/NSERROR", null);
			return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
					ResultCode.NSERROR);
		}

		int sendCount = pkeySkeyListMap.size();
		RequestCommandCollection rcc = new RequestCommandCollection();
		Map<Object, Map<Object, Result<DataEntry>>> resultMap = new HashMap<Object, Map<Object, Result<DataEntry>>>();
		DataEntryLocalCache cache = localCacheMap.get(namespace);

		boolean allHit = true;
		for (Map.Entry<? extends Serializable, ? extends List<? extends Serializable>> entry : pkeySkeyListMap
				.entrySet()) {
			Serializable pkey = entry.getKey();
			MixedKey key = null;
			RequestPrefixGetsPacket packet = new RequestPrefixGetsPacket(
					transcoder);
			packet.setNamespace((short) namespace);
			boolean hasMissed = false;
			for (Serializable skey : entry.getValue()) {
				key = new MixedKey(transcoder, pkey, skey);
				if (cache != null) {
					CacheEntry cacheEntry = cache.get(key);
					if (cacheEntry != null) {
						Map<Object, Result<DataEntry>> onePKeyResultMap = resultMap
								.get(pkey);
						if (onePKeyResultMap == null) {
							onePKeyResultMap = new HashMap<Object, Result<DataEntry>>();
							resultMap.put(pkey, onePKeyResultMap);
						}
						if (cacheEntry.status == Status.NOTEXIST) {
							onePKeyResultMap.put(skey, new Result<DataEntry>(
									ResultCode.DATANOTEXSITS));
						} else if (cacheEntry.status == Status.EXIST) {
							onePKeyResultMap.put(skey, new Result<DataEntry>(
									ResultCode.SUCCESS, cacheEntry.data));
						}
					} else {
						packet.addKey(key);
						hasMissed = true;
					}
				} else {
					// ~ local cache disabled
					packet.addKey(key);
					hasMissed = true;
				}
			}
			// ~ all hit for this pkey
			if (!hasMissed) {
				continue;
			}
			allHit = false;
			int ec = packet.encode();
			if (ec == 1) {
				MonitorLog.addStat(clientVersion,
						"mprefixGets/error/KEYTOLARGE", null);
				return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
						ResultCode.KEYTOLARGE);
			} else if (ec == 3) {
				MonitorLog.addStat(clientVersion,
						"mprefixGets/error/SERIALIZEERROR", null);
				return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
						ResultCode.SERIALIZEERROR);
			}

			long addr = configServer.getServer(transcoder.encode(key, true),
					true);
			if (addr == 0) {
				log.warn("cannot find available dataserver for specific prefix key");
				continue;
			}
			rcc.addPrefixRequest(addr, packet);
		}

		if (allHit) {
			return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
					ResultCode.SUCCESS, resultMap);
		}

		TairSendRequestStatus status = new TairSendRequestStatus();
		boolean ret = (configServer != null && configServer.isAllDead()) ? multiSender
				.sendMultiRequest(namespace, rcc, timeout, 0, getGroupName(), status)
				: multiSender.sendMultiRequest(namespace, rcc, timeout,
						timeout, getGroupName(), status);
		if (!ret) {
			log.error("some of the packets sent have no response");
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failure has happened over 100 times, sync configuration");
			}
		}
		ResponsePrefixGetsPacket resp = null;
		ResultCode rc = ResultCode.SUCCESS;
		int maxConfigVersion = 0;
		int recvCount = 0;
		for (BasePacket bp : rcc.getResultList()) {
			if (bp instanceof ResponsePrefixGetsPacket) {
				resp = (ResponsePrefixGetsPacket) bp;
				resp.decode();
				Object pkey = resp.getPKey();
				if (resultMap.get(pkey) == null) {
					resultMap.put(pkey,
							new HashMap<Object, Result<DataEntry>>());
				}
				resultMap.get(pkey).putAll(resp.getEntryMap());
				if (cache != null) {
					for (Map.Entry<Object, Result<DataEntry>> entry : resp
							.getEntryMap().entrySet()) {
						if (entry.getValue().getRc() == ResultCode.SUCCESS) {
							cache.put(new MixedKey(pkey, entry.getKey()),
									new CacheEntry(entry.getValue().getValue(),
											Status.EXIST));
						} else if (entry.getValue().getRc() == ResultCode.DATANOTEXSITS
								|| entry.getValue().getRc() == ResultCode.DATAEXPIRED) {
							cache.put(new MixedKey(pkey, entry.getKey()),
									new CacheEntry(null, Status.NOTEXIST));
						}
					}
				}
				++recvCount;
				if (resp.getConfigVersion() > maxConfigVersion) {
					maxConfigVersion = resp.getConfigVersion();
				}
			} else {
				log.warn("unexpected packet received" + bp);
			}
		}
		if (recvCount == 0) {
			rc = ResultCode.CONNERROR;
			if (status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		} else if (recvCount < sendCount) {
			rc = ResultCode.PARTSUCC;
		}
		this.checkConfigVersion(maxConfigVersion);

		return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(rc,
				resultMap);
	}

	public Result<Map<Object, Map<Object, Result<DataEntry>>>> mprefixGetHiddens(
			int namespace,
			Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap) {
		if (!this.inited) {
			return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			MonitorLog.addStat(clientVersion,
					"mprefixGetHiddens/error/NSERROR", null);
			return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
					ResultCode.NSERROR);
		}

		int sendCount = pkeySkeyListMap.size();
		RequestCommandCollection rcc = new RequestCommandCollection();
		Map<Object, Map<Object, Result<DataEntry>>> resultMap = new HashMap<Object, Map<Object, Result<DataEntry>>>();

		for (Map.Entry<? extends Serializable, ? extends List<? extends Serializable>> entry : pkeySkeyListMap
				.entrySet()) {
			Serializable pkey = entry.getKey();
			MixedKey key = null;
			RequestPrefixGetHiddensPacket packet = new RequestPrefixGetHiddensPacket(
					transcoder);
			for (Serializable skey : entry.getValue()) {
				key = new MixedKey(transcoder, pkey, skey);
				packet.addKey(key);
				packet.setNamespace((short) namespace);
			}

			int ec = packet.encode();
			if (ec == 1) {
				MonitorLog.addStat(clientVersion,
						"mprefixGetHiddens/error/KEYTOLARGE", null);
				return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
						ResultCode.KEYTOLARGE);
			} else if (ec == 3) {
				MonitorLog.addStat(clientVersion,
						"mprefixGetHiddens/error/SERIALIZEERROR", null);
				return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(
						ResultCode.SERIALIZEERROR);
			}

			long addr = serverId;

			if (!this.isDirect) {
				addr = configServer.getServer(transcoder.encode(key, true),
						true);
			}
			if (addr == 0) {
				log.warn("cannot find available dataserver for specific prefix key");
				continue;
			}
			rcc.addPrefixRequest(addr, packet);
		}

		TairSendRequestStatus status = new TairSendRequestStatus();
		boolean ret = (configServer != null && configServer.isAllDead()) ? multiSender
				.sendMultiRequest(namespace, rcc, timeout, 0, getGroupName(), status)
				: multiSender.sendMultiRequest(namespace, rcc, timeout,
						timeout, getGroupName(), status);
		if (!ret) {
			log.error("some of the packets sent have no response");
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failure has happened over 100 times, sync configuration");
			}
		}
		ResponsePrefixGetsPacket resp = null;
		ResultCode rc = ResultCode.SUCCESS;
		int maxConfigVersion = 0;
		int recvCount = 0;
		for (BasePacket bp : rcc.getResultList()) {
			if (bp instanceof ResponsePrefixGetsPacket) {
				resp = (ResponsePrefixGetsPacket) bp;
				resp.decode();
				Object pkey = resp.getPKey();
				resultMap.put(pkey, resp.getEntryMap());
				++recvCount;
				if (resp.getConfigVersion() > maxConfigVersion) {
					maxConfigVersion = resp.getConfigVersion();
				}
			} else {
				log.warn("unexpected packet received" + bp);
			}
		}
		if (recvCount == 0) {
			rc = ResultCode.CONNERROR;
			if (status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		} else if (recvCount < sendCount) {
			rc = ResultCode.PARTSUCC;
		}
		this.checkConfigVersion(maxConfigVersion);

		return new Result<Map<Object, Map<Object, Result<DataEntry>>>>(rc,
				resultMap);
	}

	/*
	 * SUCCESS, or INVAL_CONN_ERROR
	 * 
	 * @NOTE: if no invalid server available, mdelete is used.
	 */
	public ResultCode minvalid(int namespace, List<? extends Object> keys) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		InvalidServer invalidServer = invalidServerManager
				.chooseInvalidServer();
		if (invalidServer == null) {
			log.debug("no invalid server available, use mdelete instead");
			return mdelete(namespace - this.getNamespaceOffset(), keys);
		}
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "minvalid/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		log.debug("send minvalid request to " + invalidServer.getAddress());
		long s = System.currentTimeMillis();
		RequestInvalidPacket packet = new RequestInvalidPacket(transcoder,
				groupName);
		packet.setNamespace((short) namespace);
		for (Object key : keys) {
			packet.addKey(key);
		}

		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog
					.addStat(clientVersion, "minvalid/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "minvalid/error/SERIALIZEERROR",
					null);
			return ResultCode.SERIALIZEERROR;
		}

		BasePacket returnPacket = null;
		ResultCode rc = ResultCode.CONNERROR;

		do {
			invalidLocalCache(namespace, keys);
			TairClient client = null;
			try {
				client = createClient(invalidServer.getAddress(), timeout,
						timeout, packetStreamer);
				if (client != null) {
					returnPacket = (BasePacket) client.invoke(namespace,
							packet, timeout, getGroupName());
				} else {
					int value = failCounter.addAndGet(1);
					if (value > maxFailCount) {
						this.checkConfigVersion(0);
						failCounter.set(0);
					}
				}
			} catch (Exception e) {
				// handle the Timeout Exception by logGuardee
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + client + " failed ", e);
					}
				}
				int value = failCounter.addAndGet(1);
				if (value > maxFailCount) {
					this.checkConfigVersion(0);
					failCounter.set(0);
				}
			}

			if (returnPacket == null) {
				invalidServer.failed();
				break;
			} else {
				invalidServer.successed();
			}

			if (returnPacket instanceof ReturnPacket) {
				rc = ResultCode
						.valueOf(((ReturnPacket) returnPacket).getCode());
				if (((ReturnPacket) returnPacket).getCode() == 0) {
					// configServer.updateInvalidFailCount(address, 0L);
				} else {
					MonitorLog.addStat(clientVersion, "minvalid/exception",
							null);
				}
				// ReturnPacket r = (ReturnPacket)returnPacket;
				// configServer.checkConfigVersion(r.getConfigVersion());
			}
		} while (false);

		long e = System.currentTimeMillis();

		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "minvalid", returnPacket
					.getRemoteAddress().toString() + "$" + namespace, (e - s),
					1);
		} else {
			MonitorLog.addStat(clientVersion, "minvalid", null, (e - s), 1);
		}
		return rc;
	}

	public Result<DataEntry> get(int namespace, Serializable key) {
		if (!this.inited) {
			return new Result<DataEntry>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		// check namesapce
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		ResultCode resultCode = ResultCode.SUCCESS;
		DataEntry resultObject = null;
		// first, check localcache
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			CacheEntry entry = cache.get(key);
			if (entry != null) {
				MonitorLog.addStat(clientVersion, "get/localcache/hit", null);
				if (entry.status == Status.NOTEXIST) {
					resultCode = ResultCode.DATANOTEXSITS;
					resultObject = null;
				} else if (entry.status == Status.EXIST) {
					resultCode = ResultCode.SUCCESS;
					resultObject = entry.data;
				}
				return new Result<DataEntry>(resultCode, resultObject);
			}
			MonitorLog.addStat(clientVersion, "get/localcache/miss", null);
		} else {
			// System.out.println("@@@no cache found");
		}

		//这里应该是发消息去取redis上的内容
		RequestGetPacket packet = new RequestGetPacket(transcoder);
		packet.setNamespace((short) namespace);
		packet.addKey(key);
		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "get/error/KEYTOLARGE", null);
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "get/error/SERIALIZEERROR", null);
			return new Result<DataEntry>(ResultCode.SERIALIZEERROR);
		}

		long s = System.currentTimeMillis();
		TairSendRequestStatus status = new TairSendRequestStatus();
        BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status);
		if (returnPacket == null) {
			resultCode = ResultCode.CONNERROR;
			MonitorLog.addStat(clientVersion, "get/exception", null);
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
		} else if (!(returnPacket instanceof ResponseGetPacket)) {
			resultCode = ResultCode.CONNERROR;
			log.error("failed cast " + returnPacket.getClass() + "  to "
					+ ResponseGetPacket.class);

		} else {
			ResponseGetPacket response = (ResponseGetPacket) returnPacket;
			List<DataEntry> entryList = response.getEntryList();

			resultCode = ResultCode.valueOf(response.getResultCode());
			int hit = 0;
			if (resultCode == ResultCode.SUCCESS && entryList.size() > 0) {
				resultObject = entryList.get(0);
				hit = 1;
			}


			//这东西会抛出异常吗？
			this.checkConfigVersion(response.getConfigVersion());

			/**
			 * @author xiaodu
			 */
			long e = System.currentTimeMillis();
			if (returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "get", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), (e - s), 1);
				MonitorLog.addStat(clientVersion, "get/hit", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), hit, 1);
				MonitorLog.addStat(clientVersion, "get/len", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), returnPacket.getLen(), 1);
			} else {
				MonitorLog.addStat(clientVersion, "get", null, (e - s), 1);
			}

		}
		// add result to localcache
		if (cache != null) {
			if (resultCode == ResultCode.DATANOTEXSITS
					|| resultCode == ResultCode.DATAEXPIRED) {
				cache.put(key, new CacheEntry(null, Status.NOTEXIST));
			} else if (resultCode == ResultCode.SUCCESS) {
				cache.put(key, new CacheEntry(resultObject, Status.EXIST));
			}
		}
		return new Result<DataEntry>(resultCode, resultObject);
	}

	public Result<DataEntry> get(int namespace, Serializable key, int expireTime) {
		if (!this.inited) {
			return new Result<DataEntry>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		if (expireTime < 0) {
			return new Result<DataEntry>(ResultCode.INVALIDARG);
		}

		RequestGetExpirePacket packet = new RequestGetExpirePacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setExpireTime(expireTime);
		packet.addKey(key);

		int ec = packet.encode();

		if (ec == 1) {
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status);

		if ((returnPacket != null)
				&& returnPacket instanceof ResponseGetExpirePacket) {
			ResponseGetExpirePacket r = (ResponseGetExpirePacket) returnPacket;

			DataEntry resultObject = null;

			List<DataEntry> entryList = r.getEntryList();

			rc = ResultCode.valueOf(r.getResultCode());

			if (rc == ResultCode.SUCCESS && entryList.size() > 0)
				resultObject = entryList.get(0);

			configServer.checkConfigVersion(r.getConfigVersion());

			return new Result<DataEntry>(rc, resultObject);
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		return new Result<DataEntry>(rc);
	}

	public Result<DataEntry> getModifyDate(int namespace, Serializable key,
			int expireTime) {
		if (!this.inited) {
			return new Result<DataEntry>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		// check namesapce
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		ResultCode resultCode = ResultCode.SUCCESS;
		DataEntry resultObject = null;
		// first, check localcache
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			CacheEntry entry = cache.get(key);
			if (entry != null) {
				MonitorLog.addStat(clientVersion, "get/localcache/hit", null);
				if (entry.status == Status.NOTEXIST) {
					resultCode = ResultCode.DATANOTEXSITS;
					resultObject = null;

				} else if (entry.status == Status.EXIST) {
					resultCode = ResultCode.SUCCESS;
					resultObject = entry.data;
				}
				return new Result<DataEntry>(resultCode, resultObject);
			}
			MonitorLog.addStat(clientVersion, "get/localcache/miss", null);
		}

		RequestGetModifyDatePacket packet = new RequestGetModifyDatePacket(
				transcoder);
		packet.setNamespace((short) namespace);
		packet.setExpired(expireTime);

		packet.addKey(key);
		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "get/error/KEYTOLARGE", null);
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "get/error/SERIALIZEERROR", null);
			return new Result<DataEntry>(ResultCode.SERIALIZEERROR);
		}

		long s = System.currentTimeMillis();
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status);
		if (returnPacket == null) {
			resultCode = ResultCode.CONNERROR;
			MonitorLog.addStat(clientVersion, "get/exception", null);
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
		} else if (!(returnPacket instanceof ResponseGetModifyDatePacket)) {
			resultCode = ResultCode.CONNERROR;
			log.error("failed cast " + returnPacket.getClass() + "  to "
					+ ResponseGetModifyDatePacket.class);

		} else {
			ResponseGetModifyDatePacket response = (ResponseGetModifyDatePacket) returnPacket;
			List<DataEntry> entryList = response.getEntryList();

			resultCode = ResultCode.valueOf(response.getResultCode());
			int hit = 0;
			if (resultCode == ResultCode.SUCCESS && entryList.size() > 0) {
				resultObject = entryList.get(0);
				hit = 1;
			}

			this.checkConfigVersion(response.getConfigVersion());

			/**
			 * @author xiaodu
			 */
			long e = System.currentTimeMillis();
			if (returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "get", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), (e - s), 1);
				MonitorLog.addStat(clientVersion, "get/hit", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), hit, 1);
				MonitorLog.addStat(clientVersion, "get/len", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), returnPacket.getLen(), 1);
			} else {
				MonitorLog.addStat(clientVersion, "get", null, (e - s), 1);
			}

		}
		// add result to localcache
		if (cache != null) {
			if (resultCode == ResultCode.DATANOTEXSITS
					|| resultCode == ResultCode.DATAEXPIRED) {
				cache.put(key, new CacheEntry(null, Status.NOTEXIST));
			} else if (resultCode == ResultCode.SUCCESS) {
				cache.put(key, new CacheEntry(resultObject, Status.EXIST));
			}
		}
		return new Result<DataEntry>(resultCode, resultObject);
	}

	public Result<DataEntry> getHidden(int namespace, Serializable key) {
		if (!this.inited) {
			return new Result<DataEntry>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		long s = System.currentTimeMillis();
		RequestGetHiddenPacket packet = new RequestGetHiddenPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "getHidden/error/KEYTOLARGE",
					null);
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "getHidden/error/SERIALIZEERROR",
					null);
			return new Result<DataEntry>(ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status);

		if ((returnPacket != null) && returnPacket instanceof ResponseGetPacket) {
			ResponseGetPacket r = (ResponseGetPacket) returnPacket;
			DataEntry resultObject = null;
			List<DataEntry> entryList = r.getEntryList();
			rc = ResultCode.valueOf(r.getResultCode());
			int hit = 0;
			if (rc == ResultCode.SUCCESS && entryList.size() > 0) {
				resultObject = entryList.get(0);
				hit = 1;
			}
			this.checkConfigVersion(r.getConfigVersion());
			long e = System.currentTimeMillis();
			if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "getHidden", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), (e - s), 1);
				MonitorLog.addStat(clientVersion, "getHidden/hit", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), hit, 1);
				MonitorLog.addStat(clientVersion, "getHidden/len", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), returnPacket.getLen(), 1);
			} else {
				MonitorLog
						.addStat(clientVersion, "getHidden", null, (e - s), 1);
			}
			return new Result<DataEntry>(rc, resultObject);
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "getHidden/exception", null);
		}
		return new Result<DataEntry>(rc);
	}

	public String getVersion() {
		return clientVersion;
	}

	public Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime) {
		namespace += this.getNamespaceOffset();
		if (value < 0) {
			MonitorLog.addStat(clientVersion, "incr/error/ITEMSIZEERROR", null);
			return new Result<Integer>(ResultCode.ITEMSIZEERROR);
		}
		return addCount(namespace, key, value, defaultValue, expireTime);
	}

	// bounded counter
	public Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBound, int upperBound) {
		namespace += this.getNamespaceOffset();
		return addCount(namespace, key, value, defaultValue, expireTime,
				lowBound, upperBound);
	}

	public ResultCode setCount(int namespace, Serializable key, int count) {
		return setCount(namespace, key, count, 0, 0);
	}

	public ResultCode setCount(int namespace, Serializable key, int count,
			int version, int expireTime) {
		return setCount(namespace, key, count, version, expireTime, false);
	}

	protected ResultCode setCount(int namespace, Serializable key, int count,
			int version, int expireTime, boolean rdbSetCount) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "set/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(key);
			MonitorLog.addStat(clientVersion, "setCount/delete/localcache",
					null);
		}
		long s = System.currentTimeMillis();
		RequestPutPacket packet = new RequestPutPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		if (rdbSetCount) {
			packet.setData(count);
		} else {
			IncData value = new IncData(count);
			packet.setData(value);
		}
		packet.setRdbSetCount(rdbSetCount);
		packet.setVersion((short) version);
		packet.setExpired(expireTime);

		// set flag implicitly
		int ec = packet.encode(0, DataEntry.TAIR_ITEM_FLAG_ADDCOUNT);

		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "set/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion, "set/error/VALUETOLARGE", null);
			return ResultCode.VALUETOLARGE;
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion, "set/error/SERIALIZEERROR", null);
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode() + ", msg=" + r.getMsg());
			}

			rc = ResultCode.valueOf(r.getCode());

			this.checkConfigVersion(r.getConfigVersion());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "set/exception", null);
		}
		long e = System.currentTimeMillis();

		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "set", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "set/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), packet.getBodyLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "set", null, (e - s), 1);
		}
		return rc;
	}

	private Result<Integer> addCount(int namespace, Serializable key,
			int value, int defaultValue, int expireTime) {
		if (!this.inited) {
			return new Result<Integer>(ResultCode.CLIENT_NOT_INITED);
		}

		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "addCount/error/NSERROR", null);
			return new Result<Integer>(ResultCode.NSERROR);
		}

		// in rdb expire < 0 is important, so let's try
		// if (expireTime < 0){
		// MonitorLog.addStat(clientVersion, "addCount/error/INVALIDARG", null);
		// return new ResultDTO<Integer>(ResultCode.INVALIDARG);
		// }
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(key);
			MonitorLog.addStat(clientVersion, "addCount/delete/localcache",
					null);
		}
		long s = System.currentTimeMillis();

		RequestIncDecPacket packet = new RequestIncDecPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setCount(value);
		packet.setInitValue(defaultValue);
		packet.setExpireTime(TairUtil.getDuration(expireTime));

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog
					.addStat(clientVersion, "addCount/error/KEYTOLARGE", null);
			return new Result<Integer>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "addCount/error/VALUETOLARGE",
					null);
			return new Result<Integer>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "addCount/error/SERIALIZEERROR",
					null);
			return new Result<Integer>(ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null)) {
			if (returnPacket instanceof ResponseIncDecPacket) {
				ResponseIncDecPacket r = (ResponseIncDecPacket) returnPacket;

				rc = ResultCode.SUCCESS;

				this.checkConfigVersion(r.getConfigVersion());

				long e = System.currentTimeMillis();
				/**
				 * @author xiaodu
				 */
				if (returnPacket != null
						&& returnPacket.getRemoteAddress() != null) {
					MonitorLog.addStat(clientVersion, "addCount", returnPacket
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s), 1);
				} else {
					MonitorLog.addStat(clientVersion, "addCount", null,
							(e - s), 1);
				}
				return new Result<Integer>(rc, r.getValue());
			} else if (returnPacket instanceof ReturnPacket) {
				ReturnPacket rp = (ReturnPacket) returnPacket;
				rc = ResultCode.valueOf(rp.getCode());
				this.checkConfigVersion(rp.getConfigVersion());
			}

		} else {
			if (status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "addCount/exception", null);
		}

		return new Result<Integer>(rc);
	}

	private Result<Integer> addCount(int namespace, Serializable key,
			int value, int defaultValue, int expireTime, int lowBound,
			int upperBound) {
		if (!this.inited) {
			return new Result<Integer>(ResultCode.CLIENT_NOT_INITED);
		}

		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "addCountBounded/error/NSERROR",
					null);
			return new Result<Integer>(ResultCode.NSERROR);
		}

		if (lowBound > upperBound) {
			MonitorLog.addStat(clientVersion,
					"addCountBounded/error/SERIALIZEERROR", null);
			return new Result<Integer>(ResultCode.SERIALIZEERROR);
		}

		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(key);
			MonitorLog.addStat(clientVersion, "addCount/delete/localcache",
					null);
		}
		long s = System.currentTimeMillis();

		RequestIncDecBoundedPacket packet = new RequestIncDecBoundedPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setCount(value);
		packet.setInitValue(defaultValue);
		packet.setExpireTime(TairUtil.getDuration(expireTime));
		packet.setLowBound(lowBound);
		packet.setUpperBound(upperBound);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"addCountBounded/error/KEYTOLARGE", null);
			return new Result<Integer>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion,
					"addCountBounded/error/VALUETOLARGE", null);
			return new Result<Integer>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"addCountBounded/error/SERIALIZEERROR", null);
			return new Result<Integer>(ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null)) {
			if (returnPacket instanceof ResponseIncDecBoundedPacket) {
				ResponseIncDecBoundedPacket r = (ResponseIncDecBoundedPacket) returnPacket;

				rc = ResultCode.SUCCESS;

				this.checkConfigVersion(r.getConfigVersion());

				long e = System.currentTimeMillis();
				/**
				 * @author xiaodu
				 */
				if (returnPacket != null
						&& returnPacket.getRemoteAddress() != null) {
					MonitorLog.addStat(clientVersion, "addCountBounded",
							returnPacket.getRemoteAddress().toString() + "$"
									+ namespace + "$" + this.getGroupName(),
							(e - s), 1);
				} else {
					MonitorLog.addStat(clientVersion, "addCountBounded", null,
							(e - s), 1);
				}
				return new Result<Integer>(rc, r.getValue());
			} else if (returnPacket instanceof ReturnPacket) {
				ReturnPacket rp = (ReturnPacket) returnPacket;
				rc = ResultCode.valueOf(rp.getCode());
				this.checkConfigVersion(rp.getConfigVersion());
			}

		} else {
			if (status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog
					.addStat(clientVersion, "addCountBounded/exception", null);
		}

		return new Result<Integer>(rc);
	}

	public ResultCode mdelete(int namespace, List<? extends Object> keys) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "mdelete/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		long s = System.currentTimeMillis();
		RequestCommandCollection rcc = new RequestCommandCollection();

		DataEntryLocalCache cache = localCacheMap.get(namespace);
		for (Object key : keys) {

			if (cache != null) {
				cache.del(key);
				MonitorLog.addStat(clientVersion, "mdelete/localcache", null);
			}

			long address = this.serverId;

			if (!this.isDirect) {
				address = configServer.getServer(transcoder.encode(key), false);
			}

			if (address == 0) {
				continue;
			}

			RequestRemovePacket packet = (RequestRemovePacket) rcc
					.findRequest(address);

			if (packet == null) {
				packet = new RequestRemovePacket(transcoder);
				packet.setNamespace((short) namespace);
				packet.addKey(key);
				rcc.addRequest(address, packet);
			} else {
				packet.addKey(key);
			}
		}

		for (BasePacket p : rcc.getRequestCommandMap().values()) {
			RequestGetPacket rp = (RequestGetPacket) p;
			// check key size
			int ec = rp.encode();

			if (ec == 1) {
				log.error("key too larget: ");
				MonitorLog.addStat(clientVersion, "mdelete/error/KEYTOLARGE",
						null);
				return ResultCode.KEYTOLARGE;
			} else if (ec == 3) {
				log.error("serialize error: ");
				MonitorLog.addStat(clientVersion,
						"mdelete/error/SERIALIZEERROR", null);
				return ResultCode.SERIALIZEERROR;
			}
		}

		ResultCode resultCode = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		boolean ret = (configServer != null && configServer.isAllDead()) ? multiSender
				.sendRequest(namespace, rcc, timeout, 0, getGroupName(), status)
				: multiSender.sendRequest(namespace, rcc, timeout, timeout,
						getGroupName(), status);
		long e = System.currentTimeMillis();
		if (ret) {
			int maxConfigVersion = 0;

			resultCode = ResultCode.SUCCESS;
			int resultCount = rcc.getResultList().size();
			for (BasePacket rp : rcc.getResultList()) {
				if (rp instanceof ReturnPacket) {
					ReturnPacket returnPacket = (ReturnPacket) rp;
					returnPacket.decode();

					if (returnPacket.getConfigVersion() > maxConfigVersion) {
						maxConfigVersion = returnPacket.getConfigVersion();
					}

					ResultCode drc = ResultCode.valueOf(returnPacket.getCode());
					if (drc.isSuccess() == false && drc != ResultCode.NSERROR) {
						log.debug("mdelete not return success, result code: "
								+ ResultCode.valueOf(returnPacket.getCode()));
						resultCode = ResultCode.PARTSUCC;
						MonitorLog.addStat(clientVersion,
								"mdelete/error/PARTSUCC", null);
					} else if (drc == ResultCode.NSERROR) {
						resultCode = ResultCode.NSERROR;
					}
				}

				/**
				 * multi: to get multiple times, average the response time when
				 * all return
				 * 
				 * @author xiaodu
				 */
				if (rp != null && rp.getRemoteAddress() != null) {
					if (resultCount != 0) {
						MonitorLog.addStat(clientVersion, "mdelete", rp
								.getRemoteAddress().toString()
								+ "$"
								+ namespace + "$" + this.getGroupName(),
								(e - s) / resultCount, 1);
					} else {
						MonitorLog.addStat(clientVersion, "mdelete", rp
								.getRemoteAddress().toString()
								+ "$"
								+ namespace + "$" + this.getGroupName(),
								(e - s), 1);
					}
				} else {
					if (resultCount != 0)
						MonitorLog.addStat(clientVersion, "mdelete", null,
								(e - s) / resultCount, 1);
				}
			}

			this.checkConfigVersion(maxConfigVersion);
		} else {
			MonitorLog.addStat(clientVersion, "mdelete/exception", null);

			// failure counter, when reaching 100 times, force checking the
			// config version
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		if (resultCode == ResultCode.SUCCESS
				|| resultCode == ResultCode.PARTSUCC) {
			invalidLocalCache(namespace, keys);
		}
		return resultCode;
	}

	private void invalidLocalCache(Integer namespace,
			List<? extends Object> keys) {
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			for (Object o : keys) {
				cache.del(o);
			}
		}
	}

	private void invalidLocalCache(Integer namespace, Object key) {
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null)
			cache.del(key);
	}

	// public ResultDTO<List<DataEntry>> mget(int namespace,
	// List<? extends Object> keys) {
	// return mget(namespace, keys, false);
	// }

	public Result<List<DataEntry>> mget(int namespace,
			List<? extends Object> keys) {
		if (!this.inited) {
			return new Result<List<DataEntry>>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "mget/error/NSERROR", null);
			return new Result<List<DataEntry>>(ResultCode.NSERROR);
		}
		long s = System.currentTimeMillis();
		RequestCommandCollection rcc = new RequestCommandCollection();

		List<DataEntry> results = new ArrayList<DataEntry>();

		List<Object> tempKeys = new ArrayList<Object>();
		tempKeys.addAll(keys);

		DataEntryLocalCache localCache = localCacheMap.get(namespace);
		if (localCache != null) {
			Iterator<? extends Object> iter = tempKeys.iterator();
			while (iter.hasNext()) {
				Object o = iter.next();
				CacheEntry entry = localCache.get(o);
				if (entry != null) {
					if (entry.status == Status.NOTEXIST) {
						iter.remove();
					} else if (entry.status == Status.EXIST) {
						results.add(entry.data);
						iter.remove();
					}
				}
			}
		}

		for (Object key : tempKeys) {
			long address = this.serverId;

			if (!this.isDirect) {
				address = configServer.getServer(transcoder.encode(key), true);
			}

			if (address == 0) {
				continue;
			}

			RequestGetPacket packet = (RequestGetPacket) rcc
					.findRequest(address);

			if (packet == null) {
				packet = new RequestGetPacket(transcoder);
				packet.setNamespace((short) namespace);
				packet.addKey(key);
				rcc.addRequest(address, packet);
			} else {
				packet.addKey(key);
			}
		}

		int reqSize = results.size();

		for (BasePacket p : rcc.getRequestCommandMap().values()) {
			RequestGetPacket rp = (RequestGetPacket) p;

			// calculate uniq key number
			reqSize += rp.getKeyList().size();

			// check key size
			int ec = rp.encode();

			if (ec == 1) {
				log.error("key too larget: ");
				MonitorLog
						.addStat(clientVersion, "mget/error/KEYTOLARGE", null);
				return new Result<List<DataEntry>>(ResultCode.KEYTOLARGE);
			} else if (ec == 3) {
				log.error("serialize error: ");
				MonitorLog.addStat(clientVersion, "mget/error/SERIALIZEERROR",
						null);
				return new Result<List<DataEntry>>(ResultCode.SERIALIZEERROR);
			}
		}

		TairSendRequestStatus status = new TairSendRequestStatus();
		boolean ret = (configServer != null && configServer.isAllDead()) ? multiSender
				.sendRequest(namespace, rcc, timeout, 0, getGroupName(), status)
				: multiSender.sendRequest(namespace, rcc, timeout, timeout,
						getGroupName(), status);
		if (!ret) {
			MonitorLog.addStat(clientVersion, "mget/exception", null);

			// failure counter, when reaching 100 times, force checking the
			// config version
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}

			ResultCode resultCode = ResultCode.CONNERROR;
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
			return new Result<List<DataEntry>>(resultCode);
		}

		ResultCode rc = ResultCode.SUCCESS;
		ResponseGetPacket resp = null;

		int maxConfigVersion = 0;
		long e = System.currentTimeMillis();
		int resultCount = rcc.getResultList().size();
		for (BasePacket bp : rcc.getResultList()) {
			int hit = 0;
			if (bp instanceof ResponseGetPacket) {
				resp = (ResponseGetPacket) bp;
				resp.decode();
				List<DataEntry> entryList = resp.getEntryList();
				results.addAll(entryList);
				if (localCache != null) {
					for (DataEntry entry : entryList) {
						localCache.put(entry.getKey(), new CacheEntry(entry,
								Status.EXIST));
					}
				}
				// calculate max config version
				if (resp.getConfigVersion() > maxConfigVersion) {
					maxConfigVersion = resp.getConfigVersion();
				}
				if (resp.getEntryList().size() > 0) {
					hit = 1;
				}
			} else {
				log.warn("receive wrong packet type: " + bp);
			}

			/**
			 * multi: to get multiple times, average the response time when all
			 * return
			 * 
			 * @author xiaodu
			 */
			if (bp != null && bp.getRemoteAddress() != null) {
				if (resultCount != 0) {
					MonitorLog.addStat(clientVersion, "mget", bp
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s) / resultCount,
							1);
				} else {
					MonitorLog.addStat(clientVersion, "mget", bp
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s), 1);
				}
				MonitorLog.addStat(clientVersion, "mget/len", bp
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), bp.getLen(), 1);
				MonitorLog.addStat(clientVersion, "mget/hit", bp
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), hit, 1);
			} else {
				if (resultCount != 0)
					MonitorLog.addStat(clientVersion, "mget", null, (e - s)
							/ resultCount, 1);
			}
		}

		this.checkConfigVersion(maxConfigVersion);

		if (results.size() == 0) {
			rc = ResultCode.DATANOTEXSITS;
		} else if (results.size() != reqSize) {
			if (log.isDebugEnabled()) {
				log.debug("mget partly success: request key size: " + reqSize
						+ ", get " + results.size());
			}
			MonitorLog.addStat(clientVersion, "mget/error/PARTSUCC", null);
			rc = ResultCode.PARTSUCC;
		}

		return new Result<List<DataEntry>>(rc, results);
	}

	public Result<List<DataEntry>> mgetModifyDate(int namespace,
			List<? extends Object> keys) {
		if (!this.inited) {
			return new Result<List<DataEntry>>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "mget/error/NSERROR", null);
			return new Result<List<DataEntry>>(ResultCode.NSERROR);
		}
		long s = System.currentTimeMillis();
		RequestCommandCollection rcc = new RequestCommandCollection();

		List<DataEntry> results = new ArrayList<DataEntry>();

		List<Object> tempKeys = new ArrayList<Object>();
		tempKeys.addAll(keys);

		DataEntryLocalCache localCache = localCacheMap.get(namespace);
		if (localCache != null) {
			Iterator<? extends Object> iter = tempKeys.iterator();
			while (iter.hasNext()) {
				Object o = iter.next();
				CacheEntry entry = localCache.get(o);
				if (entry != null) {
					if (entry.status == Status.NOTEXIST) {
						iter.remove();
					} else if (entry.status == Status.EXIST) {
						results.add(entry.data);
						iter.remove();
					}
				}
			}
		}

		for (Object key : tempKeys) {
			long address = this.serverId;

			if (!this.isDirect) {
				address = configServer.getServer(transcoder.encode(key), true);
			}

			if (address == 0) {
				continue;
			}

			RequestGetModifyDatePacket packet = (RequestGetModifyDatePacket) rcc
					.findRequest(address);

			if (packet == null) {
				packet = new RequestGetModifyDatePacket(transcoder);
				packet.setNamespace((short) namespace);
				packet.addKey(key);
				rcc.addRequest(address, packet);
			} else {
				packet.addKey(key);
			}
		}

		int reqSize = results.size();

		for (BasePacket p : rcc.getRequestCommandMap().values()) {
			RequestGetModifyDatePacket rp = (RequestGetModifyDatePacket) p;

			// calculate uniq key number
			reqSize += rp.getKeyList().size();

			// check key size
			int ec = rp.encode();

			if (ec == 1) {
				log.error("key too larget: ");
				MonitorLog
						.addStat(clientVersion, "mget/error/KEYTOLARGE", null);
				return new Result<List<DataEntry>>(ResultCode.KEYTOLARGE);
			} else if (ec == 3) {
				log.error("serialize error: ");
				MonitorLog.addStat(clientVersion, "mget/error/SERIALIZEERROR",
						null);
				return new Result<List<DataEntry>>(ResultCode.SERIALIZEERROR);
			}
		}

		TairSendRequestStatus status = new TairSendRequestStatus();
		boolean ret = (configServer != null && configServer.isAllDead()) ? multiSender
				.sendRequest(namespace, rcc, timeout, 0, getGroupName(), status)
				: multiSender.sendRequest(namespace, rcc, timeout, timeout,
						getGroupName(), status);
		if (!ret) {
			MonitorLog.addStat(clientVersion, "mget/exception", null);

			// failure counter, when reaching 100 times, force checking the
			// config version
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}

			ResultCode resultCode = ResultCode.CONNERROR;
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
			return new Result<List<DataEntry>>(resultCode);
		}

		ResultCode rc = ResultCode.SUCCESS;
		ResponseGetModifyDatePacket resp = null;

		int maxConfigVersion = 0;
		long e = System.currentTimeMillis();
		int resultCount = rcc.getResultList().size();
		for (BasePacket bp : rcc.getResultList()) {
			int hit = 0;
			if (bp instanceof ResponseGetModifyDatePacket) {
				resp = (ResponseGetModifyDatePacket) bp;
				resp.decode();
				List<DataEntry> entryList = resp.getEntryList();
				results.addAll(entryList);
				if (localCache != null) {
					for (DataEntry entry : entryList) {
						localCache.put(entry.getKey(), new CacheEntry(entry,
								Status.EXIST));
					}
				}
				// calculate max config version
				if (resp.getConfigVersion() > maxConfigVersion) {
					maxConfigVersion = resp.getConfigVersion();
				}
				if (resp.getEntryList().size() > 0) {
					hit = 1;
				}
			} else {
				log.warn("receive wrong packet type: " + bp);
			}

			/**
			 * multi: to get multiple times, average the response time when all
			 * return
			 * 
			 * @author xiaodu
			 */
			if (bp != null && bp.getRemoteAddress() != null) {
				if (resultCount != 0) {
					MonitorLog.addStat(clientVersion, "mget", bp
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s) / resultCount,
							1);
				} else {
					MonitorLog.addStat(clientVersion, "mget", bp
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s), 1);
				}
				MonitorLog.addStat(clientVersion, "mget/len", bp
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), bp.getLen(), 1);
				MonitorLog.addStat(clientVersion, "mget/hit", bp
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), hit, 1);
			} else {
				if (resultCount != 0)
					MonitorLog.addStat(clientVersion, "mget", null, (e - s)
							/ resultCount, 1);
			}
		}

		this.checkConfigVersion(maxConfigVersion);

		if (results.size() == 0) {
			rc = ResultCode.DATANOTEXSITS;
		} else if (results.size() != reqSize) {
			if (log.isDebugEnabled()) {
				log.debug("mget partly success: request key size: " + reqSize
						+ ", get " + results.size());
			}
			MonitorLog.addStat(clientVersion, "mget/error/PARTSUCC", null);
			rc = ResultCode.PARTSUCC;
		}

		return new Result<List<DataEntry>>(rc, results);
	}

	public ResultCode lazyRemoveArea(int namespace) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			return ResultCode.NSERROR;
		}

		RequestLazyRemoveAreaPacket packet = new RequestLazyRemoveAreaPacket();
		packet.setNamespace((short) namespace);

		packet.encode();

		boolean flag = false;
		ResultCode resultCode = ResultCode.CONNERROR;
		Set<Long> aliveNodes = configServer.getAliveNodes();
		for (Long aliveNode : aliveNodes) {
			BasePacket returnPacket = sendRequest(namespace, aliveNode, packet,
					null);
			if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
				ReturnPacket r = (ReturnPacket) returnPacket;
				if (flag == false) {
					resultCode = ResultCode.valueOf(r.getCode());
				}
				if (resultCode != ResultCode.SUCCESS) {
					flag = true;
				}
				checkConfigVersion(r.getConfigVersion());
				if (flag == true) {
					return resultCode;
				}
			}
		}
		return resultCode;
	}

	public ResultCode put(int namespace, Serializable key, Serializable value) {
		return put(namespace, key, value, 0, 0);
	}

	public ResultCode put(int namespace, Serializable key, Serializable value,
			int version) {
		return put(namespace, key, value, version, 0);
	}

	public ResultCode put(int namespace, Serializable key, Serializable value,
			int version, int expireTime) {
		return put(namespace, key, value, version, expireTime, true, false);
	}

	public ResultCode put(int namespace, Serializable key, Serializable value,
			int version, int expireTime, boolean fill_cache) {
		return put(namespace, key, value, version, expireTime, fill_cache,
				false);
	}

	public ResultCode put(int namespace, Serializable key, Serializable value,
			int version, int expireTime, boolean fill_cache, boolean backup_put) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}
		if (isTestFlow()) {
			expireTime = TESTFLOW_PERM_TAIR_EXPIRED_TIME;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > 2 * TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "put/error/PARTSUCC", null);
			return ResultCode.NSERROR;
		}

		long s = System.currentTimeMillis();
		RequestPutPacket packet = new RequestPutPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setData(value);
		packet.setVersion((short) version);
		packet.setExpired(TairUtil.getDuration(expireTime));

		// 'cause key's meta flag is meaningless when requsting to put,
		// here is a trick to set flag to DataEntry meta flag when requesting.
		int ec = packet.encode(
				fill_cache ? DataEntry.TAIR_CLIENT_PUT_FILL_CACHE_FLAG
						: DataEntry.TAIR_CLIENT_PUT_SKIP_CACHE_FLAG, 0);

		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "put/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion, "put/error/VALUETOLARGE", null);
			return ResultCode.VALUETOLARGE;
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion, "put/error/SERIALIZEERROR", null);
			return ResultCode.SERIALIZEERROR;
		} else if (ec == TairConstant.KEYORVALUEISNULL) {
			MonitorLog.addStat(clientVersion, "put/error/KEYORVALUEISNULL",
					null);
			return ResultCode.KEYORVALUEISNULL;
		}

		ResultCode resultCode = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode() + ", msg=" + r.getMsg());
			}

			resultCode = ResultCode.valueOf(r.getCode());

			this.checkConfigVersion(r.getConfigVersion());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "put/exception", null);
		}
		long e = System.currentTimeMillis();

		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "put", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "put/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), packet.getBodyLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "put", null, (e - s), 1);
		}

		if (resultCode == ResultCode.SUCCESS) {
			invalidLocalCache(namespace, key);
		}
		return resultCode;
	}

	public ResultCode mput(int namespace, List<KeyValuePack> kvRecords,
			boolean compress) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "mput/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		long s = System.currentTimeMillis();
		RequestCommandCollection rcc = new RequestCommandCollection();
		Result<Integer> result = initMPutRequesCollection(namespace, kvRecords,
				compress, rcc);
		if (!result.isSuccess() || result.getValue() <= 0) {
			MonitorLog.addStat(clientVersion,
					"mput/error/INITMPUTCOLLECTIONGERROR:" + result.getRc(),
					null);
			log.error("init mput record fail " + result + " size:"
					+ kvRecords.size());
			return result.getRc();
		}

		int reqCount = result.getValue();
		TairSendRequestStatus status = new TairSendRequestStatus();
		boolean ret = (configServer != null && configServer.isAllDead()) ? multiSender
				.sendMultiRequest(namespace, rcc, timeout, 0, getGroupName(), status)
				: multiSender.sendMultiRequest(namespace, rcc, timeout,
						timeout, getGroupName(), status);
		if (!ret) {
			MonitorLog.addStat(clientVersion, "mput/exception", null);

			// failure counter, when reaching 100 times, force checking the
			// config version
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}

			ResultCode resultCode = ResultCode.CONNERROR;
			if (status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
			return resultCode;
		}

		int maxConfigVersion = 0;
		long e = System.currentTimeMillis();
		int resultCount = rcc.getResultList().size();
		int successCount = 0;
		for (BasePacket bp : rcc.getResultList()) {
			if (bp instanceof ReturnPacket) {
				ReturnPacket resp = (ReturnPacket) bp;
				resp.decode();
				// calculate max config version
				if (resp.getConfigVersion() > maxConfigVersion) {
					maxConfigVersion = resp.getConfigVersion();
				}
				if (resp.getCode() != ResultCode.SUCCESS.getCode()) {
					// do nothing
					log.error("mput get response fail. rc: " + resp.getCode());
				} else {
					++successCount;
				}
			} else {
				log.warn("receive wrong packet type: " + bp);
			}

			/**
			 * multi: to put multiple times, average the response time when all
			 * return
			 * 
			 * @author xiaodu
			 */
			if (bp != null && bp.getRemoteAddress() != null) {
				if (resultCount != 0) {
					MonitorLog.addStat(clientVersion, "mput", bp
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s) / resultCount,
							1);
				} else {
					MonitorLog.addStat(clientVersion, "mput", bp
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s), 1);
				}
				MonitorLog.addStat(clientVersion, "mput/len", bp
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), bp.getLen(), 1);
			} else {
				if (resultCount != 0)
					MonitorLog.addStat(clientVersion, "mput", null, (e - s)
							/ resultCount, 1);
			}
		}

		this.checkConfigVersion(maxConfigVersion);

		log.debug("mput succcount " + successCount);
		ResultCode rc = ResultCode.SUCCESS;
		if (successCount == 0) {
			rc = ResultCode.SERVERERROR;
		} else if (successCount != reqCount) {
			if (log.isDebugEnabled()) {
				log.debug("mput partly success: request key size: " + reqCount
						+ ", get " + resultCount + " success " + successCount);
			}
			MonitorLog.addStat(clientVersion, "mput/error/PARTSUCC", null);
			rc = ResultCode.PARTSUCC;
		}

		return rc;
	}

	public int queryBulkWriteToken(int namespace, int bucket) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "batchwrite/error/NSERROR", null);
			return -1;
		}
		int result = -1;
		RequestQueryBulkWriteTokenPacket packet = new RequestQueryBulkWriteTokenPacket(
				transcoder);
		packet.setNamespace(namespace);
		packet.encode();

		BasePacket returnPacket = sendRequest(namespace, bucket, packet, null);
		if ((returnPacket != null)
				&& returnPacket instanceof ResponseQueryBulkWriteTokenPacket) {
			ResponseQueryBulkWriteTokenPacket r = (ResponseQueryBulkWriteTokenPacket) returnPacket;
			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode());
			}
			result = r.getTokenId();
		}
		return result;
	}

	/**
	 * for fastdump add a file
	 **/
	public ResultCode bulkWrite(int namespace, byte[] buf, int size,
			int keyCount, int bucket) {
		return bulkWrite(namespace, buf, size, keyCount, bucket, -1, false);
	}

	public ResultCode bulkWrite(int namespace, byte[] buf, int size,
			int keyCount, int bucket, boolean compress) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "batchwrite/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		final int MAX_FILE_SIZE = 10000000; // 10M
		if (size > MAX_FILE_SIZE) {
			return ResultCode.NSERROR;
		}

		int limit = 10;
		int token = -1;
		while (token < 0 && limit-- > 0) {
			token = queryBulkWriteToken(namespace, bucket);
			if (token < 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

		if (token < 0) {
			return ResultCode.WAIT_TOKEN;
		}
		return bulkWrite(namespace, buf, size, keyCount, bucket, token,
				compress);
	}

	public ResultCode bulkWrite(int namespace, byte[] buf, int size,
			int keyCount, int bucket, int token) {
		return bulkWrite(namespace, buf, size, keyCount, bucket, token, false);
	}

	public ResultCode bulkWrite(int namespace, byte[] buf, int size,
			int keyCount, int bucket, int token, boolean compress) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "batchwrite/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		final int MAX_FILE_SIZE = 10000000; // 10M
		if (size > MAX_FILE_SIZE) {
			return ResultCode.NSERROR;
		}

		if (compress) {
			// V2
			RequestBulkWriteV2Packet packet = new RequestBulkWriteV2Packet(
					transcoder);
			packet.setCompression(compress);
			packet.setKeyCount(keyCount);
			packet.setToken(token);
			packet.setNamespace(namespace);
			packet.setBucket(bucket);
			packet.setKeyCount(keyCount);
			packet.setFileContent(buf);
			packet.setFileSize(size);

			packet.encode();
			ResultCode rc = ResultCode.CONNERROR;
			BasePacket returnPacket = sendRequest(namespace, bucket, packet,
					null);

			if ((returnPacket != null)
					&& returnPacket instanceof ResponseBulkWriteV2Packet) {
				ResponseBulkWriteV2Packet r = (ResponseBulkWriteV2Packet) returnPacket;

				if (log.isDebugEnabled()) {
					log.debug("get return packet: " + returnPacket + ", code="
							+ r.getCode());
				}

				rc = ResultCode.valueOf(r.getCode());
			}
			return rc;

		} else {
			RequestBulkWritePacket packet = new RequestBulkWritePacket(
					transcoder);
			packet.setKeyCount(keyCount);
			// TODO lease
			packet.setToken(token);
			packet.setNamespace(namespace);
			packet.setBucket(bucket);
			packet.setKeyCount(keyCount);
			packet.setFileContent(buf);
			packet.setFileSize(size);

			packet.encode();

			ResultCode rc = ResultCode.CONNERROR;
			BasePacket returnPacket = sendRequest(namespace, bucket, packet,
					null);

			if ((returnPacket != null)
					&& returnPacket instanceof ResponseBulkWritePacket) {
				ResponseBulkWritePacket r = (ResponseBulkWritePacket) returnPacket;

				if (log.isDebugEnabled()) {
					log.debug("get return packet: " + returnPacket + ", code="
							+ r.getCode());
				}

				rc = ResultCode.valueOf(r.getCode());
			}
			return rc;
		}
	}

	public ResultCode putModifyDate(int namespace, Serializable key,
			Serializable value, long modifyTime) {
		return putModifyDate(namespace, key, value, modifyTime, 0, 0);
	}

	public ResultCode putModifyDate(int namespace, Serializable key,
			Serializable value, long modifyTime, int expireTime) {
		return putModifyDate(namespace, key, value, modifyTime, 0, expireTime);
	}

	public ResultCode putModifyDate(int namespace, Serializable key,
			Serializable value, long modifyTime, int version, int expireTime) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return ResultCode.NSERROR;
		}

		if (expireTime < 0 || modifyTime <= 0)
			return ResultCode.INVALIDARG;

		RequestPutModifyDatePacket packet = new RequestPutModifyDatePacket(
				transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setData(value);
		packet.setVersion((short) version);
		packet.setExpired(expireTime);
		packet.setModifyTime(modifyTime);

		int ec = packet.encode();

		if (ec == 1) {
			return ResultCode.KEYTOLARGE;
		} else if (ec == 2) {
			return ResultCode.VALUETOLARGE;
		} else if (ec == 3) {
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode rc = ResultCode.CONNERROR;

		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode() + ", msg=" + r.getMsg());
			}

			rc = ResultCode.valueOf(r.getCode());

			configServer.checkConfigVersion(r.getConfigVersion());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		return rc;
	}

	// ---------------don't use this interface before you knew it well
	// --------put a KV with epoch, if client epoch > server epoch, overwrite.
	// or else, discard it
	// --------conflit with version in put()
	public ResultCode compareAndPut(int namespace, Serializable key,
			Serializable value, short epoch) {
		return compareAndPut(namespace, key, value, epoch, 0);
	}

	public ResultCode compareAndPut(int namespace, Serializable key,
			Serializable value, short epoch, int expireTime) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return ResultCode.NSERROR;
		}

		if (expireTime < 0 || epoch < 0)
			return ResultCode.INVALIDARG;

		RequestPutPacket packet = new RequestPutPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setData(value);
		packet.setVersion(epoch);
		packet.setExpired(expireTime);

		int ec = packet.encode(TairConstant.COMPARE_AND_PUT_FLAG, 0);

		if (ec == 1) {
			return ResultCode.KEYTOLARGE;
		} else if (ec == 2) {
			return ResultCode.VALUETOLARGE;
		} else if (ec == 3) {
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode rc = ResultCode.CONNERROR;

		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode() + ", msg=" + r.getMsg());
			}

			rc = ResultCode.valueOf(r.getCode());

			configServer.checkConfigVersion(r.getConfigVersion());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		return rc;
	}

	private Result<Integer> initMPutRequesCollection(int namespace,
			List<KeyValuePack> kvRecords, boolean compress,
			RequestCommandCollection rcc) {
		// Needn't + offset
		Map<Long, Map<Integer, List<KeyValuePack>>> serverBucketRecordMap = new HashMap<Long, Map<Integer, List<KeyValuePack>>>();
		int resultCount = 0;
		int bucket = 0;
		for (KeyValuePack record : kvRecords) {
			try {
				bucket = configServer.getBucket(transcoder.encode(
						record.getKey(), true));
			} catch (Exception e) {
				return new Result<Integer>(ResultCode.SERIALIZEERROR, 0);
			}
			long serverId = configServer.getServer(bucket, false);
			// once fail, just return
			if (serverId == 0) {
				return new Result<Integer>(ResultCode.SERVERERROR, 0);
			}
			Map<Integer, List<KeyValuePack>> bucketMap = serverBucketRecordMap
					.get(serverId);
			if (bucketMap == null) {
				bucketMap = new HashMap<Integer, List<KeyValuePack>>();
				serverBucketRecordMap.put(serverId, bucketMap);
			}
			List<KeyValuePack> recordList = bucketMap.get(bucket);
			if (recordList == null) {
				recordList = new ArrayList<KeyValuePack>();
				bucketMap.put(bucket, recordList);
			}
			recordList.add(record);
		}

		for (Entry<Long, Map<Integer, List<KeyValuePack>>> serverEntry : serverBucketRecordMap
				.entrySet()) {
			for (Entry<Integer, List<KeyValuePack>> bucketEntry : serverEntry
					.getValue().entrySet()) {
				++resultCount;
				RequestMPutPacket packet = new RequestMPutPacket(transcoder);
				packet.setNamespace((short) namespace);
				packet.setRecords((List<KeyValuePack>) bucketEntry.getValue());
				int rc = 0;
				if (compress) {
					rc = packet.compress();
				}
				if (rc == 0) {
					rc = packet.encode();
				}
				if (rc == 1) {
					return new Result<Integer>(ResultCode.KEYTOLARGE, 0);
				} else if (rc == 2) {
					return new Result<Integer>(ResultCode.VALUETOLARGE, 0);
				} else if (rc == 3) {
					return new Result<Integer>(ResultCode.SERIALIZEERROR, 0);
				}
				rcc.addPrefixRequest((Long) serverEntry.getKey(), packet);
			}
		}
		return new Result<Integer>(ResultCode.SUCCESS, resultCount);
	}

	public Result<DataEntry> prefixGet(int namespace, Serializable pkey,
			Serializable sKey) {
		if (!this.inited) {
			return new Result<DataEntry>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		MixedKey key = new MixedKey(transcoder, pkey, sKey);
		DataEntry resultObject = null;
		ResultCode rc = ResultCode.SUCCESS;
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			CacheEntry entry = cache.get(key);
			if (entry != null) {
				MonitorLog.addStat(clientVersion, "prefixGet/localcache/hit",
						null);
				if (entry.status == Status.NOTEXIST) {
					rc = ResultCode.DATANOTEXSITS;
					resultObject = null;
				} else if (entry.status == Status.EXIST) {
					resultObject = entry.data;
				}
				return new Result<DataEntry>(rc, resultObject);
			}
			MonitorLog
					.addStat(clientVersion, "prefixGet/localcache/miss", null);
		}
		long s = System.currentTimeMillis();

		RequestGetPacket packet = new RequestGetPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "prefixGet/error/KEYTOLARGE",
					null);
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "prefixGet/error/VALUETOLARGE",
					null);
			return new Result<DataEntry>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "prefixGet/error/SERIALIZEERROR",
					null);
			return new Result<DataEntry>(ResultCode.SERIALIZEERROR);
		}
		rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status);

		if ((returnPacket != null) && returnPacket instanceof ResponseGetPacket) {
			ResponseGetPacket r = (ResponseGetPacket) returnPacket;

			resultObject = null;

			List<DataEntry> entryList = r.getEntryList();

			rc = ResultCode.valueOf(r.getResultCode());
			int hit = 0;
			if (rc == ResultCode.SUCCESS && entryList.size() > 0) {
				resultObject = entryList.get(0);
				MixedKey mixedKey = (MixedKey) resultObject.getKey();
				resultObject.setKey(mixedKey.getSKey());
				hit = 1;
			}

			this.checkConfigVersion(r.getConfigVersion());

			long e = System.currentTimeMillis();
			/**
			 * @author xiaodu
			 */
			if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "prefixGet", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), (e - s), 1);
				MonitorLog.addStat(clientVersion, "prefixGet/hit", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), hit, 1);
				MonitorLog.addStat(clientVersion, "prefixGet/len", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), returnPacket.getLen(), 1);
			} else {
				MonitorLog
						.addStat(clientVersion, "prefixGet", null, (e - s), 1);
			}
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "prefixGet/exception", null);
		}

		if (cache != null) {
			if (rc == ResultCode.DATANOTEXSITS || rc == ResultCode.DATAEXPIRED) {
				cache.put(key, new CacheEntry(null, Status.NOTEXIST));
			} else if (rc == ResultCode.SUCCESS) {
				cache.put(key, new CacheEntry(resultObject, Status.EXIST));
			}
		}
		return new Result<DataEntry>(rc, resultObject);
	}

	public Result<Map<Object, Result<DataEntry>>> simplePrefixGets(
			int namespace, Serializable pkey,
			List<? extends Serializable> subkeys) {
		if (!this.inited) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.NSERROR);
		}
		if (subkeys.size() == 0) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.INVALIDARG);
		}

		long s = System.currentTimeMillis();
		Map<Object, Result<DataEntry>> resultMap = new HashMap<Object, Result<DataEntry>>();

		RequestSimplePrefixGetsPacket request = new RequestSimplePrefixGetsPacket(
				transcoder);
		request.setNamespace((short) namespace);

		ResultCode code = ResultCode.SUCCESS;
		int successCount = 0;
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache == null) {
			request.addKey(pkey, subkeys);
		} else {
			ArrayList<Serializable> queryKeys = new ArrayList<Serializable>();
			for (Serializable skey : subkeys) {
				MixedKey mk = new MixedKey(null, pkey, skey);
				CacheEntry ce = cache.get(mk);
				if (ce == null) {
					queryKeys.add(skey);
					code = ResultCode.PARTSUCC;
				} else if (ce.status == CacheEntry.Status.EXIST) {
					resultMap.put(mk, new Result<DataEntry>(ResultCode.SUCCESS,
							ce.data));
					successCount++;
				} else if (ce.status == CacheEntry.Status.NOTEXIST
						|| ce.status == CacheEntry.Status.DELEDTED) {
					resultMap.put(mk, new Result<DataEntry>(
							ResultCode.DATANOTEXSITS));
					code = ResultCode.PARTSUCC;
				} else {
					queryKeys.add(skey);
					code = ResultCode.PARTSUCC;
				}
			}
			MonitorLog.addStat(clientVersion, "simplePrefixGets/hit",
					"localcachle", 0, resultMap.size());
			if (queryKeys.size() == 0) {
				return new Result<Map<Object, Result<DataEntry>>>(code,
						resultMap);
			}
			request.addKey(pkey, queryKeys);
		}

		int ec = request.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"simplePrefixGets/error/KEYTOLARGE", null);
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"simplePrefixGets/error/SERIALIZEERROR", null);
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.SERIALIZEERROR);
		}

		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, new MixedKey(
				transcoder, pkey, null), request, true, status);

		if ((returnPacket != null)
				&& returnPacket instanceof ResponseSimplePrefixGetsPacket) {
			ResponseSimplePrefixGetsPacket response = (ResponseSimplePrefixGetsPacket) returnPacket;
			this.checkConfigVersion(response.getConfigVersion());
			List<Result<DataEntry>> result = response.getResultEntryList();
			for (Result<DataEntry> subResult : result) {
				resultMap.put(subResult.getValue().getKey(), subResult);
				if (cache != null) {
					if (subResult.getRc().equals(ResultCode.SUCCESS)) {
						cache.put(subResult.getValue().getKey(),
								new CacheEntry(subResult.getValue(),
										Status.EXIST));
						successCount++;
					} else if (subResult.getRc().equals(
							ResultCode.DATANOTEXSITS)
							|| subResult.getRc().equals(ResultCode.DATAEXPIRED))
						cache.put(subResult.getValue().getKey(),
								new CacheEntry(subResult.getValue(),
										Status.NOTEXIST));
				}
				if (!subResult.getRc().equals(ResultCode.SUCCESS))
					code = ResultCode.PARTSUCC;
			}
			ResultCode responseCode = ResultCode.valueOf(response
					.getResultCode());
			if (!responseCode.equals(ResultCode.SUCCESS))
				code = responseCode;
			long e = System.currentTimeMillis();
			/**
			 * @author xiaodu
			 */
			if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "simplePrefixGets",
						returnPacket.getRemoteAddress().toString() + "$"
								+ namespace + "$" + this.getGroupName(),
						(e - s), 1);
				MonitorLog.addStat(clientVersion, "simplePrefixGets/len",
						returnPacket.getRemoteAddress().toString() + "$"
								+ namespace + "$" + this.getGroupName(),
						returnPacket.getLen(), 1);
			} else {
				MonitorLog.addStat(clientVersion, "prefsimplePrefixGetsixGets",
						null, (e - s), 1);
			}
		} else {
			MonitorLog.addStat(clientVersion, "simplePrefixGets/exception",
					null);
			code = ResultCode.CONNERROR;
			if (null == returnPacket && status.isFlowControl()) {
				code = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		if (successCount == subkeys.size())
			code = ResultCode.SUCCESS;
		return new Result<Map<Object, Result<DataEntry>>>(code, resultMap);
	}

	public Result<Map<Object, Result<DataEntry>>> prefixGets(int namespace,
			Serializable pkey, List<? extends Serializable> skeys) {
		if (!this.inited) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.NSERROR);
		}
		if (skeys.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.TOO_MANY_KEYS);
		}

		long s = System.currentTimeMillis();

		RequestPrefixGetsPacket packet = new RequestPrefixGetsPacket(transcoder);

		packet.setNamespace((short) namespace);
		MixedKey key = null;
		List<MixedKey> missedKeys = new ArrayList<MixedKey>();
		for (Serializable obj : skeys) {
			key = new MixedKey(transcoder, pkey, obj);
			missedKeys.add(key);
		}
		Map<Object, Result<DataEntry>> entryMap = new HashMap<Object, Result<DataEntry>>();
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		boolean allHit = true;
		if (cache != null) {
			Iterator<MixedKey> iter = missedKeys.iterator();
			while (iter.hasNext()) {
				MixedKey k = iter.next();
				CacheEntry entry = cache.get(k);
				if (entry != null) {
					if (entry.status == Status.NOTEXIST) {
						allHit = false;
						entryMap.put(k.getSKey(), new Result<DataEntry>(
								ResultCode.DATANOTEXSITS));
						iter.remove();
					} else if (entry.status == Status.EXIST) {
						entryMap.put(k.getSKey(), new Result<DataEntry>(
								ResultCode.SUCCESS, entry.data));
						iter.remove();
					}
				} else {
					allHit = false;
					packet.addKey(k);
				}
			}
		} else {
			for (MixedKey mkey : missedKeys) {
				packet.addKey(mkey);
			}
		}
		if (missedKeys.isEmpty()) {
			if (allHit) {
				return new Result<Map<Object, Result<DataEntry>>>(
						ResultCode.SUCCESS, entryMap);
			} else {
				return new Result<Map<Object, Result<DataEntry>>>(
						ResultCode.PARTSUCC, entryMap);
			}
		}

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "prefixGets/error/KEYTOLARGE",
					null);
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixGets/error/SERIALIZEERROR", null);
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.SERIALIZEERROR);
		}
		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status); // ~ any key would be routed
		if ((returnPacket != null)
				&& returnPacket instanceof ResponsePrefixGetsPacket) {
			ResponsePrefixGetsPacket r = (ResponsePrefixGetsPacket) returnPacket;
			Map<Object, Result<DataEntry>> resultMap = r.getEntryMap();
			entryMap.putAll(resultMap);
			if (cache != null) {
				for (Map.Entry<Object, Result<DataEntry>> entry : resultMap
						.entrySet()) {
					key = new MixedKey(pkey, entry.getKey());
					if (entry.getValue().getRc() == ResultCode.SUCCESS) {
						cache.put(key, new CacheEntry(entry.getValue()
								.getValue(), Status.EXIST));
					} else if (entry.getValue().getRc() == ResultCode.DATANOTEXSITS
							|| entry.getValue().getRc() == ResultCode.DATAEXPIRED) {
						cache.put(key, new CacheEntry(null, Status.NOTEXIST));
					}
				}
			}
			rc = ResultCode.valueOf(r.getResultCode());
			this.checkConfigVersion(r.getConfigVersion());

			long e = System.currentTimeMillis();
			/**
			 * @author xiaodu
			 */
			if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "prefixGets", returnPacket
						.getRemoteAddress().toString()
						+ "$"
						+ namespace
						+ "$"
						+ this.getGroupName(), (e - s), 1);
				MonitorLog.addStat(clientVersion, "prefixGets/len",
						returnPacket.getRemoteAddress().toString() + "$"
								+ namespace + "$" + this.getGroupName(),
						returnPacket.getLen(), 1);
			} else {
				MonitorLog.addStat(clientVersion, "prefixGets", null, (e - s),
						1);
			}
		} else {
			MonitorLog.addStat(clientVersion, "prefixGets/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		return new Result<Map<Object, Result<DataEntry>>>(rc, entryMap);
	}

	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value) {
		return prefixPut(namespace, pkey, skey, value, 0, 0);
	}

	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value, int version) {
		return prefixPut(namespace, pkey, skey, value, version, 0);
	}

	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable sKey, Serializable value, int version, int expireTime) {
		// Needn't add namespace offset
		MixedKey key = new MixedKey(transcoder, pkey, sKey);
		return put(namespace, key, value, version, expireTime);
	}

	public Result<Map<Object, ResultCode>> prefixPuts(int namespace,
			Serializable pkey, List<KeyValuePack> keyValuePacks,
			List<KeyCountPack> keyCountPacks) {
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}

		List<KeyValuePack> keyValuePackList = new ArrayList<KeyValuePack>();
		if (keyValuePacks != null) {
			for (KeyValuePack kv : keyValuePacks) {
				keyValuePackList.add(kv);
			}
		}
		if (keyCountPacks != null) {
			for (KeyCountPack kc : keyCountPacks) {
				KeyValuePack kv = new KeyValuePack(kc.getKey(), new IncData(
						kc.getCount()), kc.getVersion(), kc.getExpire());
				keyValuePackList.add(kv);
			}
		}
		if (keyValuePackList.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, ResultCode>>(ResultCode.TOO_MANY_KEYS);
		}
		long s = System.currentTimeMillis();
		RequestPrefixPutsPacket packet = new RequestPrefixPutsPacket(transcoder);
		packet.setNamespace((short) namespace);
		packet.setPKey(pkey);
		packet.setKeyValuePackList(keyValuePackList);

		Object sKey = keyValuePackList.get(0).getKey();
		MixedKey key = new MixedKey(transcoder, pkey, sKey);
		int ec = packet.encode();
		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "prefixPuts/error/keytoolarge",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion, "prefixPuts/error/valuetoolarge",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.VALUETOLARGE);
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion,
					"prefixPuts/error/serializeerror", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		Map<Object, ResultCode> keyCodeMap = null;
		if ((returnPacket != null) && returnPacket instanceof MReturnPacket) {
			MReturnPacket r = (MReturnPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			if (!rc.equals(ResultCode.SUCCESS)) {
				keyCodeMap = r.getKeyCodeMap();
			}
			this.checkConfigVersion(r.getConfigVersion());
		} else {
			MonitorLog.addStat(clientVersion, "prefixPuts/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixPuts", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "prefixPuts/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), packet.getBodyLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "prefixPuts", null, (e - s), 1);
		}

		for (KeyValuePack pack : keyValuePackList) {
			if (keyCodeMap == null || !keyCodeMap.containsKey(pack.getKey())) {
				invalidLocalCache(namespace, new MixedKey(pkey, pack.getKey()));
			}
		}
		return new Result<Map<Object, ResultCode>>(rc, keyCodeMap);
	}

	/**
	 * @param pkey
	 *            : prefix key
	 * @param keyValuePacks
	 *            : secondary keys/values, version, expire, etc.
	 * @return ResultDTO<...>.getRc().equals(ResultCode.SUCCESS) if succeed, or map
	 *         of key/resultcode of failed skeys
	 */
	public Result<Map<Object, ResultCode>> prefixPuts(int namespace,
			Serializable pkey, List<KeyValuePack> keyValuePacks) {
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}
		if (keyValuePacks.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, ResultCode>>(ResultCode.TOO_MANY_KEYS);
		}
		long s = System.currentTimeMillis();
		RequestPrefixPutsPacket packet = new RequestPrefixPutsPacket(transcoder);
		packet.setNamespace((short) namespace);
		packet.setPKey(pkey);
		packet.setKeyValuePackList(keyValuePacks);

		Object sKey = keyValuePacks.get(0).getKey();
		MixedKey key = new MixedKey(transcoder, pkey, sKey);
		int ec = packet.encode();
		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "prefixPuts/error/keytoolarge",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion, "prefixPuts/error/valuetoolarge",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.VALUETOLARGE);
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion,
					"prefixPuts/error/serializeerror", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		Map<Object, ResultCode> keyCodeMap = null;
		if ((returnPacket != null) && returnPacket instanceof MReturnPacket) {
			MReturnPacket r = (MReturnPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			if (!rc.equals(ResultCode.SUCCESS)) {
				keyCodeMap = r.getKeyCodeMap();
			}
			this.checkConfigVersion(r.getConfigVersion());
		} else {
			MonitorLog.addStat(clientVersion, "prefixPuts/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixPuts", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "prefixPuts/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), packet.getBodyLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "prefixPuts", null, (e - s), 1);
		}
		for (KeyValuePack pack : keyValuePacks) {
			if (keyCodeMap == null || !keyCodeMap.containsKey(pack.getKey())) {
				invalidLocalCache(namespace, new MixedKey(pkey, pack.getKey()));
			}
		}
		return new Result<Map<Object, ResultCode>>(rc, keyCodeMap);
	}

	public ResultCode prefixDelete(int namespace, Serializable pkey,
			Serializable sKey) {
		// Needn't add namespace offset
		MixedKey key = new MixedKey(transcoder, pkey, sKey);
		return delete(namespace, key);
	}

	/**
	 * @param pkey
	 *            : prefix key
	 * @param skeys
	 *            : secondary keys
	 * @return ResultDTO<...>.getRc().equals(ResultCode.SUCCESS) if succeed, or map
	 *         of key/resultcode of failed skeys
	 */
	public Result<Map<Object, ResultCode>> prefixDeletes(int namespace,
			Serializable pkey, List<? extends Serializable> skeys) {
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "prefixDeletes/error/NSERROR",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}

		long s = System.currentTimeMillis();
		RequestPrefixRemovesPacket packet = new RequestPrefixRemovesPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		for (Serializable key : skeys) {
			MixedKey mkey = new MixedKey(transcoder, pkey, key);
			invalidLocalCache(namespace, mkey);
			packet.addKey(mkey);
		}
		MixedKey key = new MixedKey(transcoder, pkey, skeys.get(0));

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "prefixDeletes/error/KEYTOLARGE",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion,
					"prefixDeletes/error/VALUETOLARGE", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixDeletes/error/SERIALIZEERROR", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		Map<Object, ResultCode> keyCodeMap = null;

		if ((returnPacket != null) && returnPacket instanceof MReturnPacket) {
			MReturnPacket r = (MReturnPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			if (!rc.equals(ResultCode.SUCCESS)) {
				keyCodeMap = r.getKeyCodeMap();
			}
		} else {
			MonitorLog.addStat(clientVersion, "prefixDeletes/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		long e = System.currentTimeMillis();

		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixDeletes", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog
					.addStat(clientVersion, "prefixDeletes", null, (e - s), 1);
		}

		return new Result<Map<Object, ResultCode>>(rc, keyCodeMap);
	}

	public Result<Integer> prefixIncr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime) {
		namespace += this.getNamespaceOffset();
		if (value < 0) {
			MonitorLog.addStat(clientVersion, "prefixIncr/error/ITEMSIZEERROR",
					null);
			return new Result<Integer>(ResultCode.ITEMSIZEERROR);
		}
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return addCount(namespace, key, value, defaultValue, expireTime);
	}

	public Result<Integer> prefixIncr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBounded, int upperBounded) {
		namespace += this.getNamespaceOffset();
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return addCount(namespace, key, value, defaultValue, expireTime,
				lowBounded, upperBounded);
	}

	public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace,
			Serializable pkey, List<CounterPack> packList) {
		namespace += this.getNamespaceOffset();
		for (CounterPack pack : packList) {
			int value = pack.getCount();
			if (value < 0) {
				return new Result<Map<Object, Result<Integer>>>(
						ResultCode.ITEMSIZEERROR);
			}
		}
		return prefixAddCount(namespace, pkey, packList);
	}

	public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound) {
		namespace += this.getNamespaceOffset();
		return prefixAddCount(namespace, pkey, packList, lowBound, upperBound);
	}

	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime) {
		namespace += this.getNamespaceOffset();
		if (value < 0) {
			MonitorLog.addStat(clientVersion, "prefixDecr/error/ITEMSIZEERROR",
					null);
			return new Result<Integer>(ResultCode.ITEMSIZEERROR);
		}
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return addCount(namespace, key, -value, defaultValue, expireTime);
	}

	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBound, int upperBound) {
		namespace += this.getNamespaceOffset();
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return addCount(namespace, key, -value, defaultValue, expireTime,
				lowBound, upperBound);
	}

	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList) {
		namespace += this.getNamespaceOffset();
		List<CounterPack> newPackList = new ArrayList<CounterPack>();
		for (CounterPack pack : packList) {
			int value = pack.getCount();
			if (value < 0) {
				return new Result<Map<Object, Result<Integer>>>(
						ResultCode.ITEMSIZEERROR);
			}
			CounterPack newPack = new CounterPack();
			newPack.setKey(pack.getKey());
			newPack.setCount(-value);
			newPack.setInitValue(pack.getInitValue());
			newPack.setExpire(pack.getExpire());
			newPackList.add(newPack);
		}
		return prefixAddCount(namespace, pkey, newPackList);
	}

	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound) {
		namespace += this.getNamespaceOffset();
		List<CounterPack> newPackList = new ArrayList<CounterPack>();
		for (CounterPack pack : packList) {
			CounterPack np = new CounterPack();
			np.setCount(-pack.getCount());
			np.setExpire(pack.getExpire());
			np.setInitValue(pack.getInitValue());
			np.setKey(pack.getKey());
			newPackList.add(np);
		}
		return prefixAddCount(namespace, pkey, newPackList, lowBound,
				upperBound);
	}

	private Result<Map<Object, Result<Integer>>> prefixAddCount(int namespace,
			Serializable pkey, List<CounterPack> packList) {
		if (!this.inited) {
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "prefixAddCount/error/NSERROR",
					null);
			return new Result<Map<Object, Result<Integer>>>(ResultCode.NSERROR);
		}
		if (packList.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.TOO_MANY_KEYS);
		}
		long s = System.currentTimeMillis();

		RequestPrefixIncDecPacket packet = new RequestPrefixIncDecPacket(
				transcoder);
		packet.setNamespace((short) namespace);
		packet.setPKey(pkey);
		packet.setPackList(packList);
		MixedKey key = new MixedKey(transcoder, pkey, packList.get(0).getKey());

		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCount/error/KEYTOLARGE", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCount/error/VALUETOLARGE", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCount/error/SERIALIZEERROR", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		Map<Object, Result<Integer>> resultMap = null;
		if (returnPacket != null
				&& returnPacket instanceof ResponsePrefixIncDecPacket) {
			ResponsePrefixIncDecPacket r = (ResponsePrefixIncDecPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			resultMap = r.getResultMap();
			long e = System.currentTimeMillis();
			MonitorLog.addStat(clientVersion, "prefixAddCount", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			this.checkConfigVersion(r.getConfigVersion());
		} else {
			MonitorLog.addStat(clientVersion, "prefixAddCount/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		return new Result<Map<Object, Result<Integer>>>(rc, resultMap);
	}

	// bounded counter
	private Result<Map<Object, Result<Integer>>> prefixAddCount(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound) {
		if (!this.inited) {
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCountBounded/error/NSERROR", null);
			return new Result<Map<Object, Result<Integer>>>(ResultCode.NSERROR);
		}

		if (lowBound > upperBound) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCountBounded/error/SERIALIZEERROR", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.SERIALIZEERROR);
		}
		if (packList.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.TOO_MANY_KEYS);
		}

		long s = System.currentTimeMillis();

		RequestPrefixIncDecBoundedPacket packet = new RequestPrefixIncDecBoundedPacket(
				transcoder);
		packet.setNamespace((short) namespace);
		packet.setPKey(pkey);
		packet.setPackList(packList);
		packet.setLowBound(lowBound);
		packet.setUpperBound(upperBound);
		MixedKey key = new MixedKey(transcoder, pkey, packList.get(0).getKey());

		int ec = packet.encode();
		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCountBounded/error/KEYTOLARGE", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCountBounded/error/VALUETOLARGE", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixAddCountBounded/error/SERIALIZEERROR", null);
			return new Result<Map<Object, Result<Integer>>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		Map<Object, Result<Integer>> resultMap = null;
		if (returnPacket != null
				&& returnPacket instanceof ResponsePrefixIncDecBoundedPacket) {
			ResponsePrefixIncDecBoundedPacket r = (ResponsePrefixIncDecBoundedPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			resultMap = r.getResultMap();
			long e = System.currentTimeMillis();
			MonitorLog
					.addStat(clientVersion, "prefixAddCountBounded",
							returnPacket.getRemoteAddress().toString() + "$"
									+ namespace + "$" + this.getGroupName(),
							(e - s), 1);
			this.checkConfigVersion(r.getConfigVersion());
		} else {
			MonitorLog.addStat(clientVersion,
					"prefixAddCountBounded/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		return new Result<Map<Object, Result<Integer>>>(rc, resultMap);
	}

	public ResultCode prefixSetCount(int namespace, Serializable pkey,
			Serializable skey, int count) {
		return prefixSetCount(namespace, pkey, skey, count, 0, 0);
	}

	public ResultCode prefixSetCount(int namespace, Serializable pkey,
			Serializable skey, int count, int version, int expireTime) {
		// Needn't add namespace offset
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return setCount(namespace, key, count, version, expireTime);
	}

	public Result<Map<Object, ResultCode>> prefixSetCounts(int namespace,
			Serializable pkey, List<KeyCountPack> keyCountPacks) {
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}
		List<KeyValuePack> keyValuePackList = new ArrayList<KeyValuePack>();
		for (KeyCountPack kc : keyCountPacks) {
			KeyValuePack kv = new KeyValuePack(kc.getKey(), new IncData(
					kc.getCount()), kc.getVersion(), TairUtil.getDuration(kc
					.getExpire()));
			keyValuePackList.add(kv);
		}
		if (keyValuePackList.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, ResultCode>>(ResultCode.TOO_MANY_KEYS);
		}
		long s = System.currentTimeMillis();
		RequestPrefixPutsPacket packet = new RequestPrefixPutsPacket(transcoder);
		packet.setNamespace((short) namespace);
		packet.setPKey(pkey);

		packet.setKeyValuePackList(keyValuePackList);

		Object sKey = keyCountPacks.get(0).getKey();
		MixedKey key = new MixedKey(transcoder, pkey, sKey);
		int ec = packet.encode();
		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion,
					"prefixSetCounts/error/keytoolarge", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion,
					"prefixSetCounts/error/valuetoolarge", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.VALUETOLARGE);
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion,
					"prefixSetCounts/error/serializeerror", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		Map<Object, ResultCode> keyCodeMap = null;
		if ((returnPacket != null) && returnPacket instanceof MReturnPacket) {
			MReturnPacket r = (MReturnPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			if (!rc.equals(ResultCode.SUCCESS)) {
				keyCodeMap = r.getKeyCodeMap();
			}
			this.checkConfigVersion(r.getConfigVersion());
		} else {
			MonitorLog
					.addStat(clientVersion, "prefixSetCounts/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}

		long e = System.currentTimeMillis();
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixSetCounts", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "prefixSetCounts/len",
					returnPacket.getRemoteAddress().toString() + "$"
							+ namespace + "$" + this.getGroupName(),
					packet.getBodyLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "prefixSetCounts", null, (e - s),
					1);
		}
		for (KeyCountPack pack : keyCountPacks) {
			if (keyCodeMap == null || !keyCodeMap.containsKey(pack.getKey())) {
				invalidLocalCache(namespace, new MixedKey(pkey, pack.getKey()));
			}
		}
		return new Result<Map<Object, ResultCode>>(rc, keyCodeMap);
	}

	public ResultCode prefixHide(int namespace, Serializable pkey,
			Serializable skey) {
		// Needn't add namespace offset
		MixedKey key = new MixedKey(transcoder, pkey, skey);
		return hide(namespace, key);
	}

	public Result<DataEntry> prefixGetHidden(int namespace, Serializable pkey,
			Serializable skey) {
		List<Serializable> skeys = new ArrayList<Serializable>();
		skeys.add(skey);
		Result<Map<Object, Result<DataEntry>>> result = prefixGetHiddens(
				namespace, pkey, skeys);
		if (result.getValue() != null) {
			for (Map.Entry<Object, Result<DataEntry>> entry : result.getValue()
					.entrySet()) {
				return entry.getValue();
			}
		}
		return new Result<DataEntry>(result.getRc());
	}

	public Result<Map<Object, Result<DataEntry>>> prefixGetHiddens(
			int namespace, Serializable pkey, List<? extends Serializable> skeys) {
		if (!this.inited) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.NSERROR);
		}
		if (skeys.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.TOO_MANY_KEYS);
		}

		long s = System.currentTimeMillis();

		RequestPrefixGetHiddensPacket packet = new RequestPrefixGetHiddensPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		MixedKey key = null;
		for (Serializable obj : skeys) {
			key = new MixedKey(transcoder, pkey, obj);
			packet.addKey(key);
		}

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion,
					"prefixGetHiddens/error/KEYTOLARGE", null);
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.KEYTOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixGetHiddens/error/SERIALIZEERROR", null);
			return new Result<Map<Object, Result<DataEntry>>>(
					ResultCode.SERIALIZEERROR);
		}
		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, true,
				status); // ~ any key would be routed
		Map<Object, Result<DataEntry>> entryMap = null;
		if ((returnPacket != null)
				&& returnPacket instanceof ResponsePrefixGetsPacket) {
			ResponsePrefixGetsPacket r = (ResponsePrefixGetsPacket) returnPacket;
			entryMap = r.getEntryMap();
			rc = ResultCode.valueOf(r.getResultCode());
			this.checkConfigVersion(r.getConfigVersion());

			long e = System.currentTimeMillis();
			/**
			 * @author xiaodu
			 */
			if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
				MonitorLog.addStat(clientVersion, "prefixGetHiddens",
						returnPacket.getRemoteAddress().toString() + "$"
								+ namespace + "$" + this.getGroupName(),
						(e - s), 1);
				MonitorLog.addStat(clientVersion, "prefixGetHiddens/len",
						returnPacket.getRemoteAddress().toString() + "$"
								+ namespace + "$" + this.getGroupName(),
						returnPacket.getLen(), 1);
			} else {
				MonitorLog.addStat(clientVersion, "prefixGetHiddens", null,
						(e - s), 1);
			}
			return new Result<Map<Object, Result<DataEntry>>>(rc, entryMap);
		} else {
			MonitorLog.addStat(clientVersion, "prefixGetHiddens/exception",
					null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		return new Result<Map<Object, Result<DataEntry>>>(rc, entryMap);
	}

	public Result<Map<Object, ResultCode>> prefixHides(int namespace,
			Serializable pkey, List<? extends Serializable> skeys) {
		if (!this.inited) {
			return new Result<Map<Object, ResultCode>>(
					ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog
					.addStat(clientVersion, "prefixHides/error/NSERROR", null);
			return new Result<Map<Object, ResultCode>>(ResultCode.NSERROR);
		}
		if (skeys.size() > TairConstant.MAX_KEY_COUNT) {
			return new Result<Map<Object, ResultCode>>(ResultCode.TOO_MANY_KEYS);
		}

		long s = System.currentTimeMillis();
		RequestPrefixHidesPacket packet = new RequestPrefixHidesPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		for (Serializable key : skeys) {
			packet.addKey(new MixedKey(transcoder, pkey, key));
		}
		MixedKey key = new MixedKey(transcoder, pkey, skeys.get(0));

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "prefixHides/error/KEYTOLARGE",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "prefixHides/error/VALUETOLARGE",
					null);
			return new Result<Map<Object, ResultCode>>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"prefixHides/error/SERIALIZEERROR", null);
			return new Result<Map<Object, ResultCode>>(
					ResultCode.SERIALIZEERROR);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);
		Map<Object, ResultCode> keyCodeMap = null;

		if ((returnPacket != null) && returnPacket instanceof MReturnPacket) {
			MReturnPacket r = (MReturnPacket) returnPacket;
			rc = ResultCode.valueOf(r.getCode());
			if (!rc.equals(ResultCode.SUCCESS)) {
				keyCodeMap = r.getKeyCodeMap();
			}
		} else {
			MonitorLog.addStat(clientVersion, "prefixHides/exception", null);
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		long e = System.currentTimeMillis();

		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "prefixHides", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog.addStat(clientVersion, "prefixHides", null, (e - s), 1);
		}

		return new Result<Map<Object, ResultCode>>(rc, keyCodeMap);
	}

	public ResultCode append(int namespace, byte[] key, byte[] value) {
		if (key == null || value == null) {
			return ResultCode.SERIALIZEERROR;
		} else if (this.header == true) {
			return ResultCode.TAIR_IS_NOT_SUPPORT;
		}
		return mc_ops(MCOPS.APPEND, namespace, key, value, 0, 0);
	}

	private ResultCode mc_ops(MCOPS ops, int namespace, Serializable key,
			Serializable value, int expire, int version) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "delete/error/NSERROR", null);
			return ResultCode.NSERROR;
		}

		expire = TairUtil.getDuration(expire);

		RequestMcOpsPacket packet = new RequestMcOpsPacket(transcoder);
		packet.setNamespace((short) namespace);
		packet.setVersion((short) version);
		packet.setExpire(expire);
		packet.setMcOpcode(ops.getCode());
		packet.setKey(key);
		packet.setValue(value);
		packet.setPadding(null); // i don't where used padding

		int ret = packet.encode();
		if (ret == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "mcops/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ret == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion, "mcops/error/VALUETOLARGE", null);
			return ResultCode.VALUETOLARGE;
		} else if (ret == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion, "mcops/error/SERIALIZEERROR",
					null);
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode resultCode = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		try {
			if (returnPacket instanceof ResponseMcOpsPacket) {
				resultCode = ResultCode
						.valueOf(((ResponseMcOpsPacket) returnPacket).getCode());
			} else if (returnPacket instanceof ReturnPacket) {
				resultCode = ResultCode.valueOf(((ReturnPacket) returnPacket)
						.getCode());
			} else if (null == returnPacket && status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
		} catch (Throwable e) {
			log.warn("cast err", e);
		}
		return resultCode;
	}

	// getRange/delRange only avaliable in ldb engine.
	public Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
			Serializable startsKey, Serializable endsKey, int offset,
			int limit, boolean reverse) {
		if (reverse) {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_RANGE_ALL_REVERSE);
		} else {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_RANGE_ALL);
		}
	}

	public Result<List<DataEntry>> getRangeOnlyKey(int namespace,
			Serializable prefix, Serializable startsKey, Serializable endsKey,
			int offset, int limit, boolean reverse) {
		if (reverse) {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_RANGE_KEY_ONLY_REVERSE);
		} else {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_RANGE_KEY_ONLY);
		}
	}

	public Result<List<DataEntry>> getRangeOnlyValue(int namespace,
			Serializable prefix, Serializable startsKey, Serializable endsKey,
			int offset, int limit, boolean reverse) {
		if (reverse) {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_RANGE_VALUE_ONLY_REVERSE);
		} else {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_RANGE_VALUE_ONLY);
		}
	}

	public Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
			Serializable startsKey, Serializable endsKey, int offset, int limit) {
		return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
				limit, TairConstant.CMD_RANGE_ALL);
	}

	public Result<List<DataEntry>> getRangeOnlyKey(int namespace,
			Serializable prefix, Serializable startsKey, Serializable endsKey,
			int offset, int limit) {
		return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
				limit, TairConstant.CMD_RANGE_KEY_ONLY);
	}

	public Result<List<DataEntry>> getRangeOnlyValue(int namespace,
			Serializable prefix, Serializable startsKey, Serializable endsKey,
			int offset, int limit) {
		return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
				limit, TairConstant.CMD_RANGE_VALUE_ONLY);
	}

	public Result<List<DataEntry>> delRange(int namespace, Serializable prefix,
			Serializable startsKey, Serializable endsKey, int offset,
			int limit, boolean reverse) {
		if (reverse) {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_DEL_RANGE_REVERSE);
		} else {
			return getRangeCmd(namespace, prefix, startsKey, endsKey, offset,
					limit, TairConstant.CMD_DEL_RANGE);
		}
	}

	public Result<List<DataEntry>> getRangeCmd(int namespace,
			Serializable prefix, Serializable startsKey, Serializable endsKey,
			int offset, int limit, short cmd) {
		if (!this.inited) {
			return new Result<List<DataEntry>>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			return new Result<List<DataEntry>>(ResultCode.NSERROR);
		}
		RequestGetRangePacket packet = new RequestGetRangePacket(transcoder);

		// for Compatibility
		if (startsKey != null && startsKey instanceof String
				&& ((String) startsKey).length() == 0) {
			startsKey = null;
		}
		if (endsKey != null && endsKey instanceof String
				&& ((String) endsKey).length() == 0) {
			endsKey = null;
		}

		MixedKey startKey = new MixedKey(transcoder, prefix, startsKey);
		MixedKey endKey = new MixedKey(transcoder, prefix, endsKey);

		if (limit == 0) {
			limit = TairConstant.DEFAULT_RANGE_LIMIT;
		}
		packet.setCmd(cmd);
		packet.setNamespace((short) namespace);
		packet.setOffset(offset);
		packet.setLimit(limit);
		packet.setStartKey(startKey);
		packet.setEndKey(endKey);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog
					.addStat(clientVersion, "getRange/error/KEYTOLARGE", null);
			return new Result<List<DataEntry>>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "getRange/error/VALUETOLARGE",
					null);
			return new Result<List<DataEntry>>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "getRange/error/SERIALIZEERROR",
					null);
			return new Result<List<DataEntry>>(ResultCode.SERIALIZEERROR);
		} else if (ec == 4) {
			MonitorLog.addStat(clientVersion, "getRange/error/INVALIDARG ",
					null);
			return new Result<List<DataEntry>>(ResultCode.INVALIDARG);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, startKey, packet,
				status);
		if ((returnPacket != null)
				&& returnPacket instanceof ResponseGetRangePacket) {
			ResponseGetRangePacket r = (ResponseGetRangePacket) returnPacket;
			List<DataEntry> entryList = r.getEntryList();
			short flag = r.getFlag();
			rc = ResultCode.valueOf(r.getResultCode());
			if (configServer != null) {
				configServer.checkConfigVersion(r.getConfigVersion());
			}
			return new Result<List<DataEntry>>(rc, entryList, flag);
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		return new Result<List<DataEntry>>(rc);
	}

	// @nayan. lock/unlock/mlock/munlock to lock a key
	public ResultCode lock(int namespace, Serializable key) {
		return doLock(namespace, key, RequestLockPacket.LOCK_VALUE, "lock");
	}

	public ResultCode unlock(int namespace, Serializable key) {
		return doLock(namespace, key, RequestLockPacket.UNLOCK_VALUE, "unlock");
	}

	public Result<List<Object>> mlock(int namespace, List<? extends Object> keys) {
		return doMLock(namespace, keys, RequestLockPacket.LOCK_VALUE, "mlock",
				null);
	}

	public Result<List<Object>> mlock(int namespace,
			List<? extends Object> keys, Map<Object, ResultCode> failKeysMap) {
		return doMLock(namespace, keys, RequestLockPacket.LOCK_VALUE, "mlock",
				failKeysMap);
	}

	public Result<List<Object>> munlock(int namespace,
			List<? extends Object> keys) {
		return doMLock(namespace, keys, RequestLockPacket.UNLOCK_VALUE,
				"munlock", null);
	}

	public Result<List<Object>> munlock(int namespace,
			List<? extends Object> keys, Map<Object, ResultCode> failKeysMap) {
		return doMLock(namespace, keys, RequestLockPacket.UNLOCK_VALUE,
				"munlock", failKeysMap);
	}

	private ResultCode doLock(int namespace, Serializable key, int lockType,
			String descStr) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, descStr + "/error/NSERROR", null);
			return ResultCode.NSERROR;
		}
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(key);
			MonitorLog.addStat(clientVersion, "lock/delete/localcache", null);
		}
		long s = System.currentTimeMillis();
		RequestLockPacket packet = new RequestLockPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setLockType(lockType);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, descStr + "/error/KEYTOLARGE",
					null);
			return ResultCode.KEYTOLARGE;
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			rc = ResultCode.valueOf(((ReturnPacket) returnPacket).getCode());
		} else {
			MonitorLog.addStat(clientVersion, descStr + "/exception", null);
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failed happened 100 times, sync configuration");
			}
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		long e = System.currentTimeMillis();

		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, descStr, returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog.addStat(clientVersion, descStr, null, (e - s), 1);
		}

		return rc;
	}

	private Result<List<Object>> doMLock(int namespace,
			List<? extends Object> keys, int lockType, String descStr,
			Map<Object, ResultCode> failKeysMap) {
		if (!this.inited) {
			return new Result<List<Object>>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, descStr + "/error/NSERROR", null);
			return new Result<List<Object>>(ResultCode.NSERROR);
		}
		DataEntryLocalCache cache = localCacheMap.get(namespace);
		if (cache != null) {
			cache.del(keys);
			MonitorLog
					.addStat(clientVersion, "doMlock/delete/localcache", null);
		}
		if (failKeysMap != null) {
			failKeysMap.clear();
		}
		long s = System.currentTimeMillis();

		List<Object> sucList = null;
		Set<Object> uniqueKeys = new HashSet<Object>();

		for (Object key : keys) {
			uniqueKeys.add(key);
		}

		int sucRespSize = 0;
		int maxConfigVersion = 0;
		int retCode;

		// just one by one
		for (Object key : uniqueKeys) {
			RequestLockPacket packet = new RequestLockPacket(transcoder);
			packet.setNamespace((short) namespace);
			packet.setLockType(lockType);
			packet.setKey(key);
			int ec = packet.encode();

			if (ec == 1) {
				log.error("key too larget");
				MonitorLog.addStat(clientVersion,
						descStr + "/error/KEYTOLARGE", null);
				if (failKeysMap != null) {
					failKeysMap.put(key, ResultCode.KEYTOLARGE);
				}
				continue;
			}

			TairSendRequestStatus status = new TairSendRequestStatus();
			BasePacket returnPacket = sendRequest(namespace, key, packet,
					status);

			if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
				if (((ReturnPacket) returnPacket).getConfigVersion() > maxConfigVersion) {
					maxConfigVersion = ((ReturnPacket) returnPacket)
							.getConfigVersion();
				}
				retCode = ((ReturnPacket) returnPacket).getCode();
			} else {
				retCode = ResultCode.CONNERROR.getCode();
				log.warn("receive wrong packet type: " + returnPacket);
				MonitorLog.addStat(clientVersion, descStr + "/exception", null);
				if (failCounter.incrementAndGet() > maxFailCount) {
					this.checkConfigVersion(0);
					failCounter.set(0);
					log.warn("connection failed happened 100 times, sync configuration");
				}
				if (null == returnPacket && status.isFlowControl()) {
					retCode = ResultCode.TAIR_RPC_OVERFLOW.getCode();
				}
			}

			if (retCode == ResultCode.SUCCESS.getCode()) {
				if (sucList == null) {
					sucList = new ArrayList<Object>();
				}
				sucList.add(key);
				++sucRespSize;
			} else if (failKeysMap != null) {
				failKeysMap.put(key, ResultCode.valueOf(retCode));
			}
		}

		long e = System.currentTimeMillis();

		this.checkConfigVersion(maxConfigVersion);

		if (sucRespSize > 0) {
			MonitorLog.addStat(clientVersion, descStr, null, (e - s)
					/ sucRespSize, 1);
		}

		ResultCode rc = null;
		if (sucRespSize == uniqueKeys.size()) {
			rc = ResultCode.SUCCESS;
		} else {
			if (log.isDebugEnabled()) {
				log.error(descStr + "partly success: request key size: "
						+ uniqueKeys.size() + ", fail "
						+ (uniqueKeys.size() - sucRespSize));
			}
			MonitorLog
					.addStat(clientVersion, descStr + "/error/PARTSUCC", null);
			rc = ResultCode.PARTSUCC;
		}

		return new Result<List<Object>>(rc, sucList);
	}

	// items impl
	public ResultCode addItems(int namespace, Serializable key,
			List<? extends Object> items, int maxCount, int version,
			int expireTime) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "addItems/error/NSERROR", null);
			return ResultCode.NSERROR;
		}

		if (maxCount <= 0 || expireTime < 0) {
			MonitorLog
					.addStat(clientVersion, "addItems/error/INVALIDARG", null);
			return ResultCode.INVALIDARG;
		}

		long s = System.currentTimeMillis();
		RequestAddItemsPacket packet = new RequestAddItemsPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setData(items);
		packet.setVersion((short) version);
		packet.setExpired(TairUtil.getDuration(expireTime));
		packet.setMaxCount(maxCount);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog
					.addStat(clientVersion, "addItems/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "addItems/error/VALUETOLARGE",
					null);
			return ResultCode.VALUETOLARGE;
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion, "addItems/error/SERIALIZEERROR",
					null);
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode rc = ResultCode.CONNERROR;
		BasePacket returnPacket = sendRequest(namespace, key, packet, null);
		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode() + ", msg=" + r.getMsg());
			}

			if (r.getCode() == 0) {

				long e = System.currentTimeMillis();
				if (returnPacket != null
						&& returnPacket.getRemoteAddress() != null) {
					MonitorLog.addStat(clientVersion, "addItems", returnPacket
							.getRemoteAddress().toString()
							+ "$"
							+ namespace
							+ "$" + this.getGroupName(), (e - s), 1);
					// packet length
					MonitorLog.addStat(clientVersion, "addItems/len",
							returnPacket.getRemoteAddress().toString() + "$"
									+ namespace + "$" + this.getGroupName(),
							packet.getBodyLen(), 1);
				} else {
					MonitorLog.addStat(clientVersion, "addItems", null,
							(e - s), 1);
					// packet length
					MonitorLog.addStat(clientVersion, "addItems/len", null,
							packet.getLen(), 1);
				}

				rc = ResultCode.SUCCESS;
			} else if (r.getCode() == 2) {
				MonitorLog.addStat(clientVersion, "addItems/error/VERERROR",
						null);
				rc = ResultCode.VERERROR;
			} else {
				MonitorLog.addStat(clientVersion, "addItems/error/SERVERERROR",
						null);
				rc = ResultCode.SERVERERROR;
			}

			this.checkConfigVersion(r.getConfigVersion());

		} else {
			MonitorLog.addStat(clientVersion, "addItems/exception", null);
		}

		return rc;
	}

	public Result<DataEntry> getAndRemove(int namespace, Serializable key,
			int offset, int count) {
		if (!this.inited) {
			return new Result<DataEntry>(ResultCode.CLIENT_NOT_INITED);
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "getAndRemove/error/NSERROR",
					null);
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		if (count <= 0) {
			MonitorLog.addStat(clientVersion, "getAndRemove/error/INVALIDARG",
					null);
			return new Result<DataEntry>(ResultCode.INVALIDARG);
		}
		long s = System.currentTimeMillis();
		RequestGetAndRemoveItemsPacket packet = new RequestGetAndRemoveItemsPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);
		packet.setCount(count);
		packet.setOffset(offset);
		packet.setType(Json.ELEMENT_TYPE_INVALID);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "getAndRemove/error/KEYTOLARGE",
					null);
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion,
					"getAndRemove/error/VALUETOLARGE", null);
			return new Result<DataEntry>(ResultCode.VALUETOLARGE);
		}

		ResultCode rc = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		DataEntry entry = null;
		int hit = 0;
		if ((returnPacket != null)
				&& returnPacket instanceof ResponseGetItemsPacket) {
			ResponseGetItemsPacket r = (ResponseGetItemsPacket) returnPacket;

			List<DataEntry> entryList = r.getEntryList();

			rc = ResultCode.valueOf(r.getResultCode());

			if (rc.isSuccess() && entryList.size() > 0) {
				entry = entryList.get(0);
				try {
					entry.setValue(Json.deSerialize((byte[]) entry.getValue()));
					hit = 1;
				} catch (Throwable e1) {
					log.error("ITEM SERIALIZEERROR", e1);
					MonitorLog.addStat(clientVersion,
							"getAndRemove/error/SERIALIZEERROR", null);
					rc = ResultCode.SERIALIZEERROR;
				}
			}

			this.checkConfigVersion(r.getConfigVersion());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				rc = ResultCode.TAIR_RPC_OVERFLOW;
			}
		}
		long e = System.currentTimeMillis();
		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "getAndRemove", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "getAndRemove/hit", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), hit, 1);
			MonitorLog.addStat(clientVersion, "getAndRemove/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), returnPacket.getLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "getAndRemove", null, (e - s), 1);
		}
		return new Result<DataEntry>(rc, entry);
	}

	@Deprecated
	public Result<DataEntry> getItems(int namespace, Serializable key,
			int offset, int count) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "getItems/error/NSERROR", null);
			return new Result<DataEntry>(ResultCode.NSERROR);
		}

		if (count <= 0) {
			MonitorLog
					.addStat(clientVersion, "getItems/error/INVALIDARG", null);
			return new Result<DataEntry>(ResultCode.INVALIDARG);
		}

		long s = System.currentTimeMillis();

		RequestGetItemsPacket packet = new RequestGetItemsPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);
		packet.setCount(count);
		packet.setOffset(offset);
		packet.setType(Json.ELEMENT_TYPE_INVALID);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog
					.addStat(clientVersion, "getItems/error/KEYTOLARGE", null);
			return new Result<DataEntry>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "getItems/error/VALUETOLARGE",
					null);
			return new Result<DataEntry>(ResultCode.VALUETOLARGE);
		}

		ResultCode rc = ResultCode.CONNERROR;
		BasePacket returnPacket = sendRequest(namespace, key, packet, null);

		DataEntry entry = null;
		int hit = 0;
		if ((returnPacket != null)
				&& returnPacket instanceof ResponseGetItemsPacket) {
			ResponseGetItemsPacket r = (ResponseGetItemsPacket) returnPacket;

			List<DataEntry> entryList = r.getEntryList();

			rc = ResultCode.valueOf(r.getResultCode());

			if (rc.isSuccess() && entryList.size() > 0) {
				entry = entryList.get(0);
				try {
					entry.setValue(Json.deSerialize((byte[]) entry.getValue()));
					hit = 1;
				} catch (Throwable e1) {
					log.error("ITEM SERIALIZEERROR", e1);
					rc = ResultCode.SERIALIZEERROR;
					MonitorLog.addStat(clientVersion,
							"getItems/error/SERIALIZEERROR", null);
				}
			}

			this.checkConfigVersion(r.getConfigVersion());
		}

		long e = System.currentTimeMillis();
		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "getItems", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "getItems/hit", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), hit, 1);
			MonitorLog.addStat(clientVersion, "getItems/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), returnPacket.getLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "getItems", null, (e - s), 1);
		}

		return new Result<DataEntry>(rc, entry);
	}

	@Deprecated
	public ResultCode removeItems(int namespace, Serializable key, int offset,
			int count) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "getItems/error/NSERROR", null);
			return ResultCode.NSERROR;
		}

		if (count <= 0) {
			MonitorLog
					.addStat(clientVersion, "getItems/error/INVALIDARG", null);
			return ResultCode.INVALIDARG;
		}
		long s = System.currentTimeMillis();

		RequestRemoveItemsPacket packet = new RequestRemoveItemsPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);
		packet.setCount(count);
		packet.setOffset(offset);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog
					.addStat(clientVersion, "getItems/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion, "getItems/error/VALUETOLARGE",
					null);
			return ResultCode.VALUETOLARGE;
		}

		ResultCode rc = ResultCode.CONNERROR;
		BasePacket returnPacket = sendRequest(namespace, key, packet, null);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			rc = ResultCode.valueOf(r.getCode());

			this.checkConfigVersion(r.getConfigVersion());
		}

		long e = System.currentTimeMillis();
		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "removeItems", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog.addStat(clientVersion, "removeItems", null, (e - s), 1);
		}

		return rc;
	}

	@Deprecated
	public Result<Integer> getItemCount(int namespace, Serializable key) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "getItemCount/error/NSERROR",
					null);
			return new Result<Integer>(ResultCode.NSERROR);
		}
		long s = System.currentTimeMillis();
		RequestGetItemsCountPacket packet = new RequestGetItemsCountPacket(
				transcoder);

		packet.setNamespace((short) namespace);
		packet.addKey(key);

		int ec = packet.encode();

		if (ec == 1) {
			MonitorLog.addStat(clientVersion, "getItemCount/error/KEYTOLARGE",
					null);
			return new Result<Integer>(ResultCode.KEYTOLARGE);
		} else if (ec == 2) {
			MonitorLog.addStat(clientVersion,
					"getItemCount/error/VALUETOLARGE", null);
			return new Result<Integer>(ResultCode.VALUETOLARGE);
		} else if (ec == 3) {
			MonitorLog.addStat(clientVersion,
					"getItemCount/error/SERIALLIZEERROR", null);
			throw new IllegalArgumentException("key,value can not be null");
		}

		ResultCode rc = ResultCode.SUCCESS;
		BasePacket returnPacket = sendRequest(namespace, key, packet, null);

		int count = 0;
		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			count = ((ReturnPacket) returnPacket).getCode();
			if (count < 0)
				rc = ResultCode.valueOf(count);

			this.checkConfigVersion(r.getConfigVersion());
		}
		long e = System.currentTimeMillis();
		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "getItemCount", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
		} else {
			MonitorLog.addStat(clientVersion, "getItemCount", null, (e - s), 1);
		}
		return new Result<Integer>(rc, count);
	}

	public Map<String, String> getStat(int qtype, String groupName,
			long serverId) {
		Map<String, String> temp = null;
		if (!this.isDirect) {
			temp = configServer.retrieveStat(qtype, groupName, serverId);
		}
		return temp;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public int getCompressionThreshold() {
		return compressionThreshold;
	}

	public void setCompressionThreshold(int compressionThreshold) {
		if (compressionThreshold < TairConstant.TAIR_KEY_MAX_LENTH) {
			log.warn("compress threshold must be bigger than max key length["
					+ TairConstant.TAIR_KEY_MAX_LENTH + "], you provided:["
					+ compressionThreshold + "]");
		} else {
			this.compressionThreshold = compressionThreshold;
		}
	}

	public int getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(int compressionType) {
		if ((compressionType < 0)
				|| (compressionType >= TairConstant.TAIR_COMPRESS_TYPE_NUM)) {
			log.warn("compress type invalid");
		} else {
			this.compressionType = compressionType;
		}
	}

	public List<String> getConfigServerList() {
		return configServerList;
	}

	public void setConfigServerList(List<String> configServerList) {
		if (this.isDirect) {
			throw new IllegalArgumentException();
		}

		this.configServerList = configServerList;
	}

	// the format of dataServer is like "hostname:port", eg "127.0.0.1:1234"
	public void setDataServer(String dataServer) {
		if (null != this.configServerList) {
			throw new IllegalArgumentException();
		}

		this.dataServer = dataServer;
		this.serverId = TairUtil.hostToLong(dataServer, defaultServerPort);
		this.isDirect = true;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public int getMaxWaitThread() {
		return maxWaitThread;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
		if (null != invalidServerManager)
			invalidServerManager.setTimeout(timeout);
	}

	public void setAdminTimeout(int timeout) {
		if (adminServerManager != null) {
			adminServerManager.setTimeout(timeout);
			log.warn("set admin server timeout to " + timeout);
		}
	}

	public String toString() {
		return name + " " + getVersion();
	}

	public ConfigServer getConfigServer() {
		return configServer;
	}

	class TairPutCallbackInternal implements TairCallback {
		private TairCallback cbImpl;
		private long start = System.currentTimeMillis();
		private int namespace;

		public TairPutCallbackInternal(TairCallback cb, int namespace) {
			cbImpl = cb;
			this.namespace = namespace;
		}

		public void callback(BasePacket packet) {
			if (packet == null)
				MonitorLog.addStat(clientVersion, "put/exception", null);
			else if (packet instanceof ReturnPacket) {
				ReturnPacket returnPacket = (ReturnPacket) packet;
				String key = returnPacket.getRemoteAddress().toString() + "$"
						+ namespace + "$" + getGroupName();
				MonitorLog.addStat(clientVersion, "put", key,
						System.currentTimeMillis() - start, 1);
				MonitorLog.addStat(clientVersion, "put/len", key,
						packet.getBodyLen(), 1);
				ResultCode resultCode = ResultCode.valueOf(returnPacket
						.getCode());
				if (resultCode == ResultCode.SUCCESS) {
					invalidLocalCache(namespace, key);
				}

			} else {
				log.error("can not cast " + packet.getClass()
						+ " to ReturnPacket");
			}

			if (cbImpl != null) {
				cbImpl.callback(packet);
			}
		}

		public void callback(Exception e) {
			MonitorLog.addStat(clientVersion, "put/exception", e.toString());
			if (cbImpl != null) {
				cbImpl.callback(e);
			}
		}
	}

	public ResultCode putAsync(int namespace, Serializable key,
			Serializable value) {
		return putAsync(namespace, key, value, TairConstant.NOT_CARE_VERSION,
				TairConstant.NOT_CARE_EXPIRE, true, null);
	}

	public ResultCode putAsync(int namespace, Serializable key,
			Serializable value, int version, int expireTime, boolean fillCache,
			TairCallback cb) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "put/error/PARTSUCC", null);
			return ResultCode.NSERROR;
		}

		RequestPutPacket packet = new RequestPutPacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setData(value);
		packet.setVersion((short) version);
		packet.setExpired(expireTime);

		int ec = packet.encode(
				fillCache ? DataEntry.TAIR_CLIENT_PUT_FILL_CACHE_FLAG
						: DataEntry.TAIR_CLIENT_PUT_SKIP_CACHE_FLAG, 0);

		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "put/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog.addStat(clientVersion, "put/error/VALUETOLARGE", null);
			return ResultCode.VALUETOLARGE;
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion, "put/error/SERIALIZEERROR", null);
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode resultCode = sendAsyncRequest(namespace, key, packet, false,
				new TairPutCallbackInternal(cb, namespace),
				SERVER_TYPE.DATA_SERVER, new TairSendRequestStatus());
		return resultCode;
	}

	public void close() {
		if (!sharedClientFactory && clientFactory != null)
			clientFactory.close();

		if (!isDirect && csUpdater != null)
			csUpdater.close();

		if (null != invalidServerManager)
			invalidServerManager.close();

		destroyAllLocalCache();

		// //close Monitor log non-deamon thread
		// try {
		// Field f =
		// MonitorLogContainers.class.getDeclaredField("writerThread");
		// f.setAccessible(true);
		// Thread t = (Thread)f.get(null);
		// t.stop();
		// log.warn("close MonitorLog thread");
		// } catch (Exception e) {
		// // ate it
		// //e.printStackTrace();
		// }
	}

	public static void Destroy(DefaultTairManager obj) {
		log.warn("Destroy all Tair Resources");
		obj.close();
		TairClient.Destroy();
	}

	public static void Restart() {
		TairClient.Start();
	}

	public void setMaxFailCount(int failCount) {
		maxFailCount = failCount;
	}

	public int getMaxFailCount() {
		return maxFailCount;
	}

	public void setForceService(boolean force) {
		this.forceService = force;
		if (configServer != null) {
			configServer.setForceService(force);
		}
	}

	public void setCheckDownNodes(boolean check) {
		this.checkDownNodes = check;
		if (configServer != null) {
			configServer.setCheckDownNodes(check);
		}
	}

	public int getConfigVersion() {
		return configServer.getConfigVersion();
	}

	public int getBucketCount() {
		return configServer.getBucketCount();
	}

	public Transcoder getTranscoder() {
		return transcoder;
	}

	public int getBucketOfKey(Serializable key) {
		return getBucketOfKey(key, false);
	}

	public int getBucketOfKey(Serializable key, boolean isPrefixKey) {
		if (isPrefixKey) {
			MixedKey pkey = new MixedKey(transcoder, key, null);
			return TairUtil.getBucketOfKey(pkey, configServer.getBucketCount(),
					transcoder);
		} else {
			return TairUtil.getBucketOfKey(key, configServer.getBucketCount(),
					transcoder);
		}
	}

	public Map<String, String> retrieveConfigMap() {
		return configServer.grabGroupConfigMap();
	}

	public List<Integer> getBucketByServer(long serverId) {
		List<Long> serverList = configServer.getServerList();
		int bucket_count = configServer.getBucketCount();
		List<Integer> buckets = new ArrayList<Integer>();
		for (int i = 0; i < bucket_count; ++i) {
			if (serverList.get(i) == serverId) {
				buckets.add(i);
			}
		}
		return buckets;
	}

	// cmd to configserver
	public List<String> getNsStatus(List<String> groups) {
		List<String> retValues = new ArrayList<String>();
		if (opCmdToCs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_GET_AREA_STATUS
						.value(),
				groups, retValues).isSuccess()) {
			return retValues;
		} else {
			return null;
		}
	}

	public List<String> getGroupStatus(List<String> groups) {
		List<String> retValues = new ArrayList<String>();
		if (opCmdToCs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_GET_GROUP_STATUS
						.value(),
				groups, retValues).isSuccess()) {
			return retValues;
		} else {
			return null;
		}
	}

	public ResultCode setNsStatus(String group, int namespace, String status) {
		if (group == null || status == null) {
			return ResultCode.INVALIDARG;
		}
		List<String> params = new ArrayList<String>();
		params.add(group);
		params.add(namespace + "");
		params.add(status);
		return opCmdToCs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_SET_AREA_STATUS
						.value(),
				params, null);
	}

	public ResultCode setGroupStatus(String group, String status) {
		if (group == null || status == null) {
			return ResultCode.INVALIDARG;
		}
		List<String> params = new ArrayList<String>();
		params.add(group);
		params.add(status);
		return opCmdToCs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_SET_GROUP_STATUS
						.value(),
				params, null);
	}

	public List<String> getTmpDownServer(List<String> groups) {
		List<String> retValues = new ArrayList<String>();
		if (opCmdToCs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_GET_TMP_DOWN_SERVER
						.value(),
				groups, retValues).isSuccess()) {
			return retValues;
		} else {
			return null;
		}
	}

	public ResultCode resetServer(String group, String dataServer) {
		if (group == null) {
			return ResultCode.INVALIDARG;
		}
		List<String> params = new ArrayList<String>();
		params.add(group);
		if (dataServer != null) {
			params.add(dataServer);
		} else if (configServer.getServerList() != null) {
			Set<Long> serverids = new HashSet<Long>();
			for (Long id : configServer.getServerList()) {
				if (!serverids.contains(id)) {
					serverids.add(id);
					params.add(TairUtil.idToAddress(id.longValue()));
				}
			}
		}
		return opCmdToCs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_RESET_DS.value(),
				params, null);
	}

	// cmd to dataserver
	public ResultCode flushMmt(String group, String dataServer) {
		return flushMmt(group, dataServer, 0); // flush all namespace
	}

	public ResultCode flushMmt(String group, String dataServer, int namespace) {
		if (group == null) {
			return ResultCode.INVALIDARG;
		}
		if (namespace > TairConstant.NAMESPACE_MAX || namespace < 0) {
			return ResultCode.INVALIDARG;
		}
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");
		return opCmdToDs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_FLUSH_MMT.value(),
				group, dataServer, params);
	}

	public ResultCode dropAll(int namespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}
		Set<Long> serverids = null;
		if (isDirect) {
			serverids = new HashSet<Long>();
			serverids.add(serverId);
		} else {
			serverids = configServer.getAliveNodes();
		}
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");

		ResultCode resultCode = ResultCode.SUCCESS;
		for (Long serverid : serverids) {
			if (serverid != null) {
				ResultCode rc = opCmdToDs(
						TairConstant.ServerCmdType.TAIR_SERVER_CMD_DROP_AREA_DATA
								.value(), /* group name */null, TairUtil
								.idToAddress(serverid), params);
				log.warn("dropall,  namespace: " + namespace + " result: " + rc);
				if (!rc.equals(ResultCode.SUCCESS)) {
					resultCode = rc;
					break;
				}
			}
		}
		return resultCode;
	}

	public ResultCode clearEmbeddedCache(int namespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}
		Set<Long> serverids = null;
		if (isDirect) {
			serverids = new HashSet<Long>();
			serverids.add(serverId);
		} else {
			serverids = configServer.getAliveNodes();
		}
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");

		ResultCode resultCode = ResultCode.SUCCESS;
		for (Long serverid : serverids) {
			if (serverid != null) {
				ResultCode rc = opCmdToDs(
						TairConstant.ServerCmdType.TAIR_SERVER_CMD_CLEAR_MDB
								.value(), /* group name */
						null, TairUtil.idToAddress(serverid), params);
				log.warn("clean mdb,  namespace: " + namespace + " result: "
						+ rc);
				if (!rc.equals(ResultCode.SUCCESS)) {
					resultCode = rc;
					break;
				}
			}
		}
		return resultCode;
	}

	public ResultCode resetDb(String group, String dataServer, int namespace) {
		if (group == null) {
			return ResultCode.INVALIDARG;
		}
		if (namespace >= TairConstant.NAMESPACE_MAX || namespace <= 0) {
			return ResultCode.INVALIDARG;
		}
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");
		return opCmdToDs(
				TairConstant.ServerCmdType.TAIR_SERVER_CMD_RESET_DB.value(),
				group, dataServer, params);
	}

	// never used
	// private int getFirstNamespace(int namespace){
	// return namespace + 1;
	// }

	public int getNextNamespace(int masterArea) {
		List<String> params = new ArrayList<String>();
		List<String> retValues = new ArrayList<String>();
		params.add(masterArea + "");
		params.add(this.groupName);
		int currentAvailableArea = -1;
		try {
			ResultCode rc = opCmdToAdmin(
					TairConstant.ServerCmdType.TAIR_ADMIN_SERVER_CMD_GET_NEXT_AREAMAP
							.value(), params, retValues);
			if (rc.getCode() == ResultCode.SUCCESS.getCode()) {
				Iterator<String> iter = retValues.iterator();
				String adminVersion = iter.next();
				int masterAreaNow = Integer.parseInt(iter.next());
				currentAvailableArea = Integer.parseInt(iter.next());
				if (masterAreaNow == masterArea) {
					log.warn("get_next_area success, master area: "
							+ masterArea + "->" + currentAvailableArea
							+ " adminVersion: " + adminVersion);

				} else {
					log.error("get_next_area not match, master area:  "
							+ masterArea + "!=" + masterAreaNow
							+ " adminVersion: " + adminVersion);
				}
			} else {
				log.error("opcmd failed " + rc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return currentAvailableArea;
	}

	private int getPrevNamespace(int namespace) {
		int realNamespace = getMapedNamespace(namespace
				- this.getNamespaceOffset());
		if (realNamespace == -1) {
			return -1;
		}
		int oldNamespace;
		if (realNamespace % fastdumpReservedNamespace == 1) {
			oldNamespace = realNamespace + this.fastdumpNamespaceGroupNumer - 2;
		} else {
			oldNamespace = realNamespace - 1;
		}
		return oldNamespace;
	}

	public boolean setFastdumpNamespaceGroupNum(int num) {
		log.error("use initNamespace instead");
		return false;
	}

	/**
	 * get a new namespace for dump. (and clear it)
	 * 
	 * @param namespace
	 *            : namespace for read
	 * @return new namespace for dump or -1 (failed)
	 */
	public int resetNamespace(int namespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}

		int newNamespace = getNextNamespace(namespace);
		if (newNamespace == -1) {
			log.error("can not reset available namespace");
			return newNamespace;
		}

		// clear newNamespace
		Set<Long> serverids = null;
		if (isDirect) {
			serverids = new HashSet<Long>();
			serverids.add(serverId);
		} else {
			serverids = configServer.getAliveNodes();
		}
		RequestRemoveArea request = new RequestRemoveArea(transcoder);
		request.setArea(newNamespace);

		request.encode();
		for (Long id : serverids) {
			// loop until success or failed 10 times.
			int errorCount = 0;
			while (true) {
				ReturnPacket ret = (ReturnPacket) sendRequest(newNamespace, id,
						request, null);
				if (ret == null || ret.getCode() != 0) {
					log.warn("clear area " + namespace + " failed ip:"
							+ TairUtil.idToAddress(id));
					errorCount++;
					if (errorCount > 10) {
						log.error("clear area " + newNamespace
								+ " failed more than 10 times");
						return -1;
					}
				} else {
					break;
				}
			}
		}
		log.warn("resetNamespace. delall success. ns:" + newNamespace);
		return newNamespace;
	}

	/**
	 * map a namespace for read.
	 * 
	 * @param namespace
	 *            : namespace for read
	 * @param dumpnamespace
	 *            : namespace to be switching
	 */
	public ResultCode mapNamespace(int namespace, int dumpNamespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)
				|| (dumpNamespace < 0)
				|| (dumpNamespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}

		List<String> params = new ArrayList<String>();
		List<String> retValues = new ArrayList<String>();
		params.add(namespace + "");
		params.add(dumpNamespace + "");
		params.add(this.groupName);
		ResultCode rc = opCmdToAdmin(
				TairConstant.ServerCmdType.TAIR_ADMIN_SERVER_CMD_SET_AREA_MAP
						.value(),
				params, retValues);
		log.warn("mapping namespace from " + namespace + " to " + dumpNamespace
				+ " " + rc);

		try {
			Thread.sleep(3000);
		} catch (Exception e) {

		}
		return rc;
	}

	/**
	 * rollback to a old namespace for read.
	 * 
	 * @param namespace
	 *            : namespace for read
	 * @return old namespace
	 */
	public int rollbackNamespace(int namespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}

		int oldNamespace = getPrevNamespace(namespace);

		ResultCode rc = mapNamespace(namespace - this.getNamespaceOffset(),
				oldNamespace);
		if (rc.getCode() == ResultCode.SUCCESS.getCode()) {
			log.warn("rollback " + namespace + " to " + oldNamespace
					+ " success " + rc);
			return oldNamespace;
		} else {
			return -1;
		}
	}

	/**
	 * get current namespace actually pointting to.
	 * 
	 * @param namespace
	 *            : namespace for read
	 * @return namespace for write
	 */
	public int getMapedNamespace(int namespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || namespace > TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException("namespace illegal");
		}
		List<String> params = new ArrayList<String>();
		List<String> retValues = new ArrayList<String>();
		params.add(namespace + "");
		params.add(this.groupName);
		try {
			ResultCode rc = opCmdToAdmin(
					TairConstant.ServerCmdType.TAIR_ADMIN_SERVER_CMD_GET_AREA_MAP
							.value(), params, retValues);
			if (rc.getCode() == ResultCode.SUCCESS.getCode()) {
				Iterator<String> iter = retValues.iterator();
				String adminVersion = iter.next();
				int oldNamespace = Integer.parseInt(iter.next());
				int newNamespace = Integer.parseInt(iter.next());
				if (oldNamespace == namespace) {
					log.warn("get_area_map success: " + namespace + "->"
							+ newNamespace + " adminVersion: " + adminVersion);
					return newNamespace;
				} else {
					log.error("get_area_map not match : " + namespace + "!="
							+ oldNamespace + " adminVersion: " + adminVersion);
				}
			} else {
				log.error("opcmd failed " + rc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	public ResultCode cleanMD5Cache(String group) {
		Set<Long> serverids = null;
		if (isDirect) {
			serverids = new HashSet<Long>();
			serverids.add(serverId);
		} else {
			serverids = configServer.getAliveNodes();
		}
		ResultCode resultCode = ResultCode.SUCCESS;
		List<String> params = new ArrayList<String>();
		for (Long serverid : serverids) {
			if (serverid != null) {
				ResultCode rc = opCmdToDs(
						TairConstant.ServerCmdType.TAIR_SERVER_CMD_CLEAN_MD5_CACHE
								.value(), group,
						TairUtil.idToAddress(serverid), params);
				log.warn("clean md5 cache,  group: " + group + ", ds: "
						+ TairUtil.idToAddress(serverid) + " result: " + rc);
				if (!rc.equals(ResultCode.SUCCESS)) {
					resultCode = rc;
					log.error("clean md5 cache error,  group: " + group
							+ ", ds: " + TairUtil.idToAddress(serverid)
							+ " result: " + rc);
					break;
				}
			}
		}
		return resultCode;
	}

	/**
	 * check namespace gc done or not
	 * 
	 * @param namespace
	 *            : namespace to check
	 * @return status GC_STATUS_FINISHED:finished
	 *         GC_STATUS_UNFINISHED:UNFINISHED -2:FAILED, maybe server error
	 */
	public ResultCode queryGcStatus(int namespace) {
		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "batchwrite/error/NSERROR", null);
			return ResultCode.GC_STATUS_FAILED;
		}

		RequestQueryGcStatusPacket packet = new RequestQueryGcStatusPacket(
				transcoder);
		packet.setNamespace(namespace);
		packet.encode();

		Set<Long> serverids = null;
		if (isDirect) {
			serverids = new HashSet<Long>();
			serverids.add(serverId);
		} else {
			serverids = configServer.getAliveNodes();
		}
		int gcFinishedCount = 0;
		for (Long id : serverids) {
			int errorCount = 0;
			while (true) {
				BasePacket returnPacket = sendRequest(namespace, id, packet,
						null);
				if ((returnPacket != null)
						&& returnPacket instanceof ResponseQueryGcStatusPacket) {
					ResponseQueryGcStatusPacket r = (ResponseQueryGcStatusPacket) returnPacket;
					ResultCode subrc = ResultCode.valueOf(r.getCode());
					if (!(subrc.equals(ResultCode.GC_STATUS_FINISHED) || subrc
							.equals(ResultCode.GC_STATUS_UNFINISHED))) {
						log.warn("check gc_status query " + namespace
								+ " failed ip:" + TairUtil.idToAddress(id)
								+ " return code: " + subrc);
						errorCount++;
						if (errorCount > 10) {
							log.error("check gc_status query " + namespace
									+ " failed more than 10 times");
							return ResultCode.GC_STATUS_FAILED;
						}
					} else if (subrc.equals(ResultCode.GC_STATUS_UNFINISHED)) {
						return ResultCode.GC_STATUS_UNFINISHED;
					} else {
						gcFinishedCount++;
						break;
					}
				} else {
					log.error("wrong packet or no response");
					return ResultCode.GC_STATUS_FAILED;
				}
			}
		}
		if (gcFinishedCount == serverids.size()) {
			return ResultCode.GC_STATUS_FINISHED;
		} else {
			return ResultCode.GC_STATUS_UNFINISHED;
		}
	}

	public ResultCode opCmdToAdmin(int cmdType, List<String> params,
			List<String> retValues) {
		if (adminServerManager == null) {
			log.error("adminServerManager has not inited");
			return ResultCode.SERVERERROR;
		}
		String adminAddr = adminServerManager.getAddr();
		if (adminAddr == null) {
			log.error("adminServerAddr is null in group.conf");
			return ResultCode.SERVERERROR;
		}
		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(cmdType);
		request.setCmdParams(params);
		request.encode();
		ResponseOpCmdPacket returnPacket = null;
		try {
			TairClient client = createClient(adminAddr, timeout, timeout,
					packetStreamer);
			returnPacket = (ResponseOpCmdPacket) client.invoke(0, request,
					timeout, getGroupName());
		} catch (Exception e) {
			if (e instanceof TairTimeoutException) {
				if (logGuardee.guardException((TairTimeoutException) e)) {
					log.error("send request to " + adminAddr + " failed ", e);
				}
			} else {
				log.error("op cmd to admin fail: " + adminAddr, e);
			}
		}

		if (returnPacket == null) {
			log.error("send op cmd to admin fail");
			return ResultCode.SERVERERROR;
		}

		ResultCode rc = new ResultCode(returnPacket.getCode());
		if (retValues != null && returnPacket.getValues() != null) {
			retValues.addAll(returnPacket.getValues());
		}
		return rc;
	}

	public ResultCode opCmdToCs(int cmdType, List<String> params,
			List<String> retValues) {
		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(cmdType);
		request.setCmdParams(params);
		request.encode();

		ResponseOpCmdPacket returnPacket = null;
		for (String addr : configServerList) {
			try {
				TairClient client = createClient(addr, timeout, timeout,
						packetStreamer);
				returnPacket = (ResponseOpCmdPacket) client.invoke(0, request,
						timeout, getGroupName());
				break;
			} catch (Exception e) {
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + addr + " failed ", e);
					}
				} else {
					log.error("op cmd to cs fail: " + addr, e);
				}
			}
		}

		if (returnPacket == null) {
			log.error("send op cmd to cs fail");
			return ResultCode.SERVERERROR;
		}

		ResultCode rc = new ResultCode(returnPacket.getCode());
		if (returnPacket.getCode() == ResultCode.SUCCESS.getCode()
				&& retValues != null && returnPacket.getValues() != null) {
			retValues.addAll(returnPacket.getValues());
		}
		return rc;
	}

	public ResultCode opCmdToDs(int cmdType, String group, String dataServer,
			List<String> params) {
		Map<String, RequestOpCmdPacket> requestMap = new HashMap<String, RequestOpCmdPacket>();
		if (dataServer != null) {
			RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
			request.setCmdType(cmdType);
			request.setCmdParams(params);
			request.encode();
			requestMap.put(dataServer, request);
		} else {
			ConfigServer tmpConfigServer = this.configServer;

			if (!group.equals(this.groupName)) {
				// need a new configserver here
				tmpConfigServer = new ConfigServer(clientFactory, group,
						configServerList, packetStreamer, invalidServerManager,
						adminServerManager);
				if (!tmpConfigServer.retrieveConfigure()) {
					log.error("init configServer for " + group + " fail.");
					return ResultCode.SERVERERROR;
				}
			}

			List<Long> configServerList = tmpConfigServer.getServerList();
			if (configServerList == null || configServerList.isEmpty()) {
				log.error("no alive node to opcmd");
				return ResultCode.SERVERERROR;
			}

			for (long serverId : configServerList) {
				String addr = TairUtil.idToAddress(serverId);
				if (requestMap.containsKey(addr)) {
					continue;
				}
				RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
				request.setCmdType(cmdType);
				request.setCmdParams(params);
				request.encode();
				requestMap.put(addr, request);
			}
		}

		BasePacket returnPacket = null;
		int successCount = 0;
		for (Entry<String, RequestOpCmdPacket> entry : requestMap.entrySet()) {
			returnPacket = null;
			try {
				TairClient client = createClient((String) entry.getKey(),
						timeout, timeout, packetStreamer);
				returnPacket = (BasePacket) client.invoke(0,
						(BasePacket) entry.getValue(), timeout, getGroupName());
			} catch (Exception e) {
				// handle the Timeout Exception by logGuardee
				if (e instanceof TairTimeoutException) {
					if (logGuardee.guardException((TairTimeoutException) e)) {
						log.error("send request to " + (String) entry.getKey()
								+ " failed ", e);
					}
				} else {
					log.error("op cmd to ds fail: " + (String) entry.getKey(),
							e);
				}
				continue;
			}
			if (returnPacket.getPcode() == TairConstant.TAIR_RESP_RETURN_PACKET) {
				ReturnPacket response = (ReturnPacket) returnPacket;
				if (response.getCode() == ResultCode.SUCCESS.getCode()) {
					++successCount;
				}
			} else if (returnPacket.getPcode() == TairConstant.TAIR_RESP_OP_CMD_PACKET) {
				ResponseOpCmdPacket response = (ResponseOpCmdPacket) returnPacket;
				if (response.getCode() == ResultCode.SUCCESS.getCode()) {
					++successCount;
				}
			}
		}

		if (successCount == requestMap.size()) {
			return ResultCode.SUCCESS;
		} else if (requestMap.size() > 1 && successCount > 0) {
			return ResultCode.PARTSUCC;
		} else {
			return ResultCode.SERVERERROR;
		}
	}

	public synchronized Result<StatisticsResult> retrieveStatFromAllDataserver() {
		return retrieveStatFromDataserverId(configServer.getAliveNodes());
	}

	public synchronized Result<StatisticsResult> retrieveStatFromDataserver(
			Set<String> dataServer) {
		Set<Long> dataServerIds = new HashSet<Long>();
		for (String server : dataServer) {
			dataServerIds.add(new Long(TairUtil.hostToLong(server)));
		}
		return retrieveStatFromDataserverId(dataServerIds);
	}

	public synchronized Result<StatisticsResult> retrieveStatFromDataserverId(
			Set<Long> dataServerIds) {
		StatisticsResult result = new StatisticsResult();
		ResultCode rc = null;
		RequestCommandCollection rcc = new RequestCommandCollection();
		int successCount = 0;
		boolean init_statAnalyser = false;
		boolean needUpdateSchema = false;
		if (statAnalyserCache.size() == 0)
			init_statAnalyser = true;
		for (Long dsAddr : dataServerIds) {
			RequestStatisticsPacket reqPacket = new RequestStatisticsPacket();
			if (init_statAnalyser)
				reqPacket.setRetrieveSchemaFlag(true);
			rcc.addRequest(dsAddr.longValue(), reqPacket);
		}

		boolean sendPacketsReturn = (configServer != null && configServer
				.isAllDead()) ? multiSender.sendRequest(0, rcc, timeout, 0,
				getGroupName(), null) : multiSender.sendRequest(0, rcc, timeout,
				timeout, getGroupName(), null);
		int maxConfigVersion = 0;
		List<ResponseStatisticsPacket> all_resp = new ArrayList<ResponseStatisticsPacket>();
		for (BasePacket bp : rcc.getResultList())
			if (bp instanceof ResponseStatisticsPacket) {
				ResponseStatisticsPacket resp = (ResponseStatisticsPacket) bp;
				resp.decode();
				if (resp.getConfigVersion() > maxConfigVersion)
					maxConfigVersion = resp.getConfigVersion();
				// fill the statAnalyserCache
				Long respSchemaVersion = Long.valueOf(resp.getSchemaVersion());

				if (init_statAnalyser == false) { // judge if
													// resp.getSchemaVersion()
													// is in statAnalyserCache
					if (statAnalyserCache.containsKey(respSchemaVersion) == true)
						all_resp.add(resp);
					else
						needUpdateSchema = true;
				} else { // init statAnalyserCache
					all_resp.add(resp);
					if (statAnalyserCache.containsKey(respSchemaVersion) == false) {
						StatisticsAnalyser statAnalyser = new StatisticsAnalyser();
						statAnalyser.init(resp.getSchemaByte(),
								resp.getSchemaVersion());
						statAnalyserCache.put(respSchemaVersion, statAnalyser);
					}
				}
			}

		this.checkConfigVersion(maxConfigVersion);
		if (sendPacketsReturn == false) {
			log.error("some of the packets sent have no response");
			if (failCounter.incrementAndGet() > maxFailCount) {
				this.checkConfigVersion(0);
				failCounter.set(0);
				log.warn("connection failure has happened over 100 times, sync configuration");
			}
		}

		// parse stat
		for (ResponseStatisticsPacket resp : all_resp) {
			InetSocketAddress remoteAddress = (InetSocketAddress) resp
					.getRemoteAddress();
			String dsIpPortStr = remoteAddress.getAddress().getHostAddress()
					+ ":" + remoteAddress.getPort();

			StatisticsAnalyser statAnalyser = statAnalyserCache.get(Long
					.valueOf(resp.getSchemaVersion()));
			if (result.addOneDsAreaStat(dsIpPortStr, resp.getDataByte(),
					statAnalyser) == true)
				++successCount;
		}

		if (needUpdateSchema == true)
			statAnalyserCache.clear();

		if (successCount == 0)
			rc = ResultCode.UNKNOW;
		else if (successCount == dataServerIds.size())
			rc = ResultCode.SUCCESS;
		else
			rc = ResultCode.PARTSUCC;
		return new Result(rc, result);
	}

	public ResultCode expire(int namespace, Serializable key, int expiretime) {
		if (!this.inited) {
			return ResultCode.CLIENT_NOT_INITED;
		}

		namespace += this.getNamespaceOffset();
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			MonitorLog.addStat(clientVersion, "expire/error/PARTSUCC", null);
			return ResultCode.NSERROR;
		}
		if (expiretime < 0) {
			return ResultCode.INVALIDARG;
		}

		expiretime = TairUtil.getDuration(expiretime);

		long s = System.currentTimeMillis();
		RequestExpirePacket packet = new RequestExpirePacket(transcoder);

		packet.setNamespace((short) namespace);
		packet.setKey(key);
		packet.setExpire(expiretime);

		int ec = packet.encode();

		if (ec == TairConstant.KEYTOLARGE) {
			MonitorLog.addStat(clientVersion, "expire/error/KEYTOLARGE", null);
			return ResultCode.KEYTOLARGE;
		} else if (ec == TairConstant.VALUETOLARGE) {
			MonitorLog
					.addStat(clientVersion, "expire/error/VALUETOLARGE", null);
			return ResultCode.VALUETOLARGE;
		} else if (ec == TairConstant.SERIALIZEERROR) {
			MonitorLog.addStat(clientVersion, "expire/error/SERIALIZEERROR",
					null);
			return ResultCode.SERIALIZEERROR;
		}

		ResultCode resultCode = ResultCode.CONNERROR;
		TairSendRequestStatus status = new TairSendRequestStatus();
		BasePacket returnPacket = sendRequest(namespace, key, packet, status);

		if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
			ReturnPacket r = (ReturnPacket) returnPacket;

			if (log.isDebugEnabled()) {
				log.debug("get return packet: " + returnPacket + ", code="
						+ r.getCode() + ", msg=" + r.getMsg());
			}

			resultCode = ResultCode.valueOf(r.getCode());
			this.checkConfigVersion(r.getConfigVersion());
		} else {
			if (null == returnPacket && status.isFlowControl()) {
				resultCode = ResultCode.TAIR_RPC_OVERFLOW;
			}
			MonitorLog.addStat(clientVersion, "expire/exception", null);
		}
		long e = System.currentTimeMillis();

		/**
		 * @author xiaodu
		 */
		if (returnPacket != null && returnPacket.getRemoteAddress() != null) {
			MonitorLog.addStat(clientVersion, "expire", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), (e - s), 1);
			MonitorLog.addStat(clientVersion, "expire/len", returnPacket
					.getRemoteAddress().toString()
					+ "$"
					+ namespace
					+ "$"
					+ this.getGroupName(), packet.getBodyLen(), 1);
		} else {
			MonitorLog.addStat(clientVersion, "expire", null, (e - s), 1);
		}

		return resultCode;
	}

	protected void checkConfigVersion(int version) {
		if (!this.isDirect) {
			configServer.checkConfigVersion(version);
		}
	}

	public void setRefluxRatio(int ratio) {
		if (configServer != null)
			configServer.setRefluxRatio(ratio);
	}

	public void resetHappendDownServer() {
		if (configServer != null)
			configServer.reset();
	}

	public Map<String, String> notifyStat() {
		Map<String, String> stat = new HashMap<String, String>();
		stat.put("csversion", "" + configServer.getVersion());
		stat.put("csgroup", this.getGroupName().trim());
		stat.put("csaddress", "" + this.getConfigServerList());
		stat.put("HappendDownNodes", ""
				+ configServer.getHappendDownNodes().values());
		stat.put("RefluxRatio", "" + configServer.getRefluxRatio());
		return stat;
	}

	public String getTmpDownServer(String group) {
		return null;
	}

	public String getConfigId() {
		return this.groupName;
	}

	public void setCstat(CommandStatistic cstat) {
		if (null != cstat) {
			synchronized (this) {
				if (null == cstatMap) {
					cstatMap = new ConcurrentHashMap<Integer, CommandStatistic>();
				}
				CommandStatistic prev = cstatMap.putIfAbsent(
						cstat.getNamespace(), cstat);
				if (null != prev
						&& !prev.getUsername().equals(cstat.getUsername())) {
					log.error("these two username (" + prev.getUsername()
							+ ", " + cstat.getUsername()
							+ ") have same namespace and run in same process..");
				}
			}
		}
	}

	public CommandStatistic getCstat(int namespace) {
		return null == cstatMap ? null : cstatMap.get(namespace);
	}

	public String getInvalidServiceDomain() {
		return invalidServiceDomain;
	}

	public void setInvalidServiceDomain(String invalidServiceDomain) {
		this.invalidServiceDomain = invalidServiceDomain;
	}

	public String getInvalidServiceCluster() {
		return invalidServiceCluster;
	}

	public void setInvalidServiceCluster(String invalidServiceCluster) {
		this.invalidServiceCluster = invalidServiceCluster;
	}

	public boolean isHeader() {
		return header;
	}

}
