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
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.etc.TairConstant;

class ClusterHandlerNode {
	private static final Logger log = LoggerFactory.getLogger(ClusterHandlerNode.class);

	private Map<ClusterInfo, ClusterHandler> handlerMap = new HashMap<ClusterInfo, ClusterHandler>();
	private ClusterHandler[] handlers = null;
	// map bucket to avaliable cluster handler index
	private Map<Integer, List<Integer>> extraBucketMap = new HashMap<Integer, List<Integer>>();
	// map namespace to its status(on or off)
	private Map<Integer, Integer> namespaceStatusMap = new HashMap<Integer, Integer>();
	private int bucketCount = 0;
	private Transcoder transcoder = null;
	private int deadBucketCount = 0;
	private String shardingStrategy = "bucket-hash";
	private boolean surportMultiArea = true;
  private ArrayList<String> groupNames = new ArrayList<String>();

	public void setTranscoder(Transcoder transcoder) {
		this.transcoder = transcoder;
	}

	public Transcoder getTranscoder() {
		return transcoder;
	}

  public ArrayList<String> getGroupNames() {
    return groupNames;
  }

	public boolean canService() {
		return handlerMap.size() > 0 && deadBucketCount < bucketCount;
	}

	public String getMapShardStrategy() {
		return shardingStrategy;
	}

	public void setMapShardStrategy(String strategy) {
		this.shardingStrategy = strategy;
	}

	public void update(List<ClusterInfo> clusterInfos,
			ClusterHandlerNode diffHandlerNode) {
		List<Entry<ClusterInfo, ClusterHandler>> hasDownServerHandlers = new ArrayList<Entry<ClusterInfo, ClusterHandler>>();
		// construct new handlerMap
		constructHandlerMap(clusterInfos, diffHandlerNode);
		// construct new handlers
		constructHandlers(hasDownServerHandlers);
		// construct new extraBucketMap
		constructExtraBucketMap(hasDownServerHandlers);

		// if (shardingStrategy.equals()){
		// mapShardBuckets(diffHandlerNode);
		// }

		if (log.isDebugEnabled()) {
			log.debug("update :\n" + toString());
		}
	}

	public ClusterHandler pickHandler(Serializable key, int namespace) {
		if (key == null) {
			return null;
		}
		int bucket = keyToBucket(key);
		int index = bucketToHandlerIndex(bucket, namespace);
		if (log.isDebugEnabled()) {
			log.debug("key2bucket " + namespace + ":" + key + " => " + bucket);
			log.debug("bucket2index " + bucket + " => " + index);
			log.debug("pick " + bucket + " => "
					+ (index >= 0 ? handlers[index] : "none"));
		}
		return index >= 0 ? handlers[index] : null;
	}

	public ClusterHandler[] pickAllHandler() {
		return handlers;
	}

	public String toString() {
		if (handlers == null || handlers.length <= 0) {
			return "[ NO alive cluster servicing ]\n";
		}

		String[] shardBuckets = new String[handlers.length];
		int[] shardBucketCounts = new int[shardBuckets.length];
		for (int i = 0; i < handlers.length; ++i) {
			shardBuckets[i] = new String();
			shardBucketCounts[i] = 0;
		}
		String deadBuckets = "";
		int deadBucketCount = 0;

		int index = 0;
		for (int i = 0; i < bucketCount; ++i) {
			index = bucketToHandlerIndex(i, 0);
			if (index < 0) {
				deadBuckets += " " + i;
				++deadBucketCount;
			} else {
				shardBuckets[index] += " " + i;
				++shardBucketCounts[index];
			}
		}
		String result = new String();
		result += shardingStrategy + "multiarea:" + surportMultiArea
				+ " :[ buckets: " + bucketCount + ", clusters on service: "
				+ handlers.length + " ]\n" + "{\ndead buckets: "
				+ deadBucketCount + " [ " + deadBuckets + " ]\n}\n";

		for (int i = 0; i < handlers.length; ++i) {
			result += "{\n" + handlers[i] + " sharded buckets: "
					+ shardBucketCounts[i] + " [" + shardBuckets[i]
					+ " ], NsMapStatus [";
			for (Map.Entry<Integer, Integer> entry : handlers[i].getNsMap()
					.entrySet()) {
				result += entry.getKey() + ":" + entry.getValue() + ";";
			}
			result += "]\n}\n";
		}

		return result;
	}

	private int keyToBucket(Serializable key) {
		return (int) (TairUtil.murMurHash(transcoder.encode(key)) % bucketCount);
	}

	private int bucketToHandlerIndex(int bucket, int namespace) {
		if (handlers == null || handlers.length == 0) {
			log.warn("no handlers found");
			return -1;
		}

		return getIndexOfBucket(bucket, namespace);
	}

	private int hashBucket(int h) {
		h += (h << 15) ^ 0xffffcd7d;
		h ^= (h >> 10);
		h += (h << 3);
		h ^= (h >> 6);
		h += (h << 2) + (h << 14);
		return h ^ (h >> 16);
	}

	private void constructHandlerMap(List<ClusterInfo> clusterInfos,
			ClusterHandlerNode diffHandlerNode) {
		handlerMap.clear();
		if (clusterInfos.isEmpty()) {
			return;
		}

    groupNames.clear();
		for (ClusterInfo info : clusterInfos) {
      groupNames.add(info.getGroupName());
			// have been constructed
			if (handlerMap.containsKey(info)) {
				continue;
			}

			ClusterHandler handler = diffHandlerNode.handlerMap.get(info);
			// new handler
			if (handler == null) {
				handler = new ClusterHandler();
				handler.setClusterInfo(info);
				if (!handler.init()) {
					log.error("start new cluster handler fail, ignore this cluster: "
							+ info);
					continue;
				}
        log.warn("start new cluster handler success, " + info.getGroupName());
			}

			handler.reset();

			// we reuse this handler
			int newBucketCount = handler.getBucketCount();
			// must have same bucket count
			if (bucketCount > 0 && newBucketCount != bucketCount) {
				log.error("bucket count conflict: " + newBucketCount + " <> "
						+ bucketCount + ", ignore this cluster: " + info);
        handler.close();
				continue;
			}
			if (bucketCount <= 0) {
				bucketCount = newBucketCount;
			}

			Map<String, String> configMap = handler.retrieveConfigMap();
			if (configMap == null) {
				log.error("retrieve cluster config map fail, ignore this cluster: "
						+ info);
        handler.close();
  			continue;
			}

			// check cluster status
			String clusterStatus = TairUtil.parseConfig(configMap,
					TairConstant.TAIR_GROUP_STATUS);
			if (clusterStatus == null
					|| !clusterStatus
							.equalsIgnoreCase(TairConstant.TAIR_GROUP_STATUS_ON)) {
				log.info("cluster status off: " + info);
        handler.close();
				continue;
			}

			// get namespace status & construct namespace StatusMap
			namespaceStatusMap.clear();
			List<String> retValues;
			List<String> groups = new ArrayList<String>();
			groups.add(info.getGroupName());
			retValues = handler.getNsStatus(groups);
			if (retValues != null) {
				surportMultiArea = true;
				for (String statusStr : retValues) {
					// standard str: "group_1 : area_status=111,on"
					String[] strArray = statusStr.split(",|=");
					if (strArray.length == 3) {
						int namespace, status;
						namespace = Integer.parseInt(strArray[1]);
						if (strArray[2].equals("on")) {
							status = TairConstant.TAIR_NS_STATUS_ON;
						} else if (strArray[2].equals("off")) {
							status = TairConstant.TAIR_NS_STATUS_OFF;
						} else {
							status = TairConstant.TAIR_NS_STATUS_ON;
						}
						if (namespace >= 0
								&& namespace <= TairConstant.NAMESPACE_MAX
								&& status != -1) {
							handler.setNsMapStatus(namespace, status);
						} else {
							log.warn("invalid status str: " + statusStr);
						}
					} else {
						log.warn("invalid status str: " + statusStr);
					}
				}
			} else {
				surportMultiArea = false;
				log.warn("get area status failed.");
			}

			// check tmp down server
			List<String> downServers = TairUtil.parseConfig(configMap,
					TairConstant.TAIR_TMP_DOWN_SERVER,
					TairConstant.TAIR_CONFIG_VALUE_DELIMITERS);
			if (downServers != null && !downServers.isEmpty()) {
				for (String serverStr : downServers) {
					long serverId = TairUtil.hostToLong(serverStr);
					if (serverId != 0) {
						handler.addDownServer(serverId);
					} else {
						log.error("get invalid tmp down server address: "
								+ serverStr);
					}
				}
			}

			// use this cluster handler
			handlerMap.put(info, handler);
		}
	}

	private void constructHandlers(
			List<Entry<ClusterInfo, ClusterHandler>> hasDownServerHandlers) {
		if (handlerMap.isEmpty()) {
			return;
		}
		handlers = new ClusterHandler[handlerMap.size()];

		int i = 0;
		for (Entry<ClusterInfo, ClusterHandler> entry : handlerMap.entrySet()) {
			ClusterHandler handler = (ClusterHandler) entry.getValue();
			this.handlers[i] = handler;
			handler.setIndex(i++);
			if (log.isDebugEnabled()) {
			  log.debug("handler:" + entry.getValue() + "no server down:"
				  	+ handler.getDownServers().isEmpty());
			}
			if (!handler.getDownServers().isEmpty()) {
				hasDownServerHandlers.add(entry);
			}
			if (!handler.hasDownAreas()) {
				hasDownServerHandlers.add(entry);
			}
		}
	}

	private void constructExtraBucketMap(
			List<Entry<ClusterInfo, ClusterHandler>> hasDownServerHandlers) {
		extraBucketMap.clear();
		// no down server, no extrabucket
		if (hasDownServerHandlers.isEmpty()) {
			return;
		}

		// collect all down buckets by down servers
		List<Entry<ClusterInfo, ClusterHandler>> hasDownBucketHandlers = new ArrayList<Entry<ClusterInfo, ClusterHandler>>();
		collectDownBucket(hasDownServerHandlers, hasDownBucketHandlers);
		// sharding all down buckets
		shardDownBucket(hasDownBucketHandlers);
	}

	private void collectDownBucket(
			List<Entry<ClusterInfo, ClusterHandler>> hasDownServerHandlers,
			List<Entry<ClusterInfo, ClusterHandler>> hasDownBucketHandlers) {
		hasDownBucketHandlers.clear();
		if (hasDownServerHandlers.isEmpty()) {
			return;
		}

		for (Entry<ClusterInfo, ClusterHandler> entry : hasDownServerHandlers) {
			ClusterHandler handler = (ClusterHandler) entry.getValue();
			if (handler.getDownServers().isEmpty()) {
				continue;
			}

			// get downBuckets from tmp_down_server
			Set<Integer> downBuckets = handler.getDownBuckets();
			for (Long serverId : handler.getDownServers()) {
				log.debug("getbucket by server:" + serverId + ":["
						+ handler.getBucketByServer(serverId) + "]");
				downBuckets.addAll(handler.getBucketByServer(serverId));
			}

			if (!downBuckets.isEmpty()) {
				hasDownBucketHandlers.add(entry);
				log.debug("downBuckets:[" + downBuckets + "]");
			}
		}
	}

	private void shardDownBucket(
			List<Entry<ClusterInfo, ClusterHandler>> hasDownBucketHandlers) {
		extraBucketMap.clear();
		if (hasDownBucketHandlers.isEmpty()) {
			return;
		}

		// one bucket has at most handlerMap.size() shard choice.
		List<Integer> indexs = new ArrayList<Integer>(this.handlerMap.size());
		// iterator all down buckets to find handler that can service it.
		// we prefer skiping bucket's orignal cluster handler(exclude) to
		// sorting all down buckets then finding from all cluster handlers.
		for (Entry<ClusterInfo, ClusterHandler> entry : hasDownBucketHandlers) {
			Set<Integer> downBuckets = ((ClusterHandler) entry.getValue())
					.getDownBuckets();
			if (downBuckets.isEmpty()) {
				continue;
			}

			for (Integer bucket : downBuckets) {
				if (extraBucketMap.containsKey(bucket)) {
					continue;
				}
				getHandlerIndexOfBucket(bucket, (ClusterInfo) entry.getKey(),
						indexs);

				if (indexs.isEmpty()) {
					++deadBucketCount;
				}
				extraBucketMap.put(bucket, indexs);
				// extraBucketMap.put(bucket,
				// indexs.isEmpty() ? -1 :
				// indexs.get(hashBucket(bucket) % indexs.size()));
			}
		}
	}

	private void getHandlerIndexOfBucket(int bucket, ClusterInfo exclude,
			List<Integer> indexs) {
		indexs.clear();
		for (Entry<ClusterInfo, ClusterHandler> entry : this.handlerMap
				.entrySet()) {
			if (((ClusterInfo) entry.getKey()).equals(exclude)) {
				continue;
			}

			ClusterHandler handler = (ClusterHandler) entry.getValue();

			Set<Integer> downBuckets = handler.getDownBuckets();
			if (downBuckets.isEmpty() || !downBuckets.contains(bucket)) {
				indexs.add(handler.getIndex());
			}
		}
	}

	private int getIndexOfBucket(int bucket, int namespace) {

		List<Integer> indexs = extraBucketMap.get(bucket);
		List<Integer> new_indexs = new ArrayList<Integer>();;
		if (indexs != null) {
			for (Integer i : indexs) {
				if (handlers[i].getNsMapStatus(namespace) == TairConstant.TAIR_NS_STATUS_ON || !surportMultiArea) {
					new_indexs.add(i);
				}
			}
		} else { // index is null, no server down, choose on area
			for (int i = 0; i < handlers.length; i++) {
				if (handlers[i].getNsMapStatus(namespace) == TairConstant.TAIR_NS_STATUS_ON || !surportMultiArea) {
					new_indexs.add(i);
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("new_indexs :" + new_indexs.size() + "["
					+ new_indexs.toString() + "]");
		}
		return new_indexs.isEmpty() ? -1 : new_indexs.get((bucket)
				% new_indexs.size());
	}

	public void close() {
		for (Entry<ClusterInfo, ClusterHandler> entry : handlerMap.entrySet()) {
			ClusterHandler handler = (ClusterHandler) entry.getValue();
      log.warn("close clusterHandler " + entry.getKey().getGroupName());
			handler.close();
		}
		handlerMap = null;
	}
}

public class HashBucketShardClusterHandlerManager implements
		ClusterHandlerManager {

	private static final Logger log = LoggerFactory
			.getLogger(HashBucketShardClusterHandlerManager.class);

	private ClusterInfoUpdater infoUpdater = null;
	private ClusterHandlerNode current = new ClusterHandlerNode();

	public HashBucketShardClusterHandlerManager(ClusterInfoUpdater infoUpdater) {
		this.infoUpdater = infoUpdater;
		current.setTranscoder(infoUpdater.getMasterClusterHandler()
				.getTairManager().getTranscoder());
	}

  public ArrayList<String> getGroupNames() {
    return current.getGroupNames();
  }

	public boolean canService() {
		return current.canService();
	}

	public boolean update(boolean should) {
		if (should) {
			infoUpdater.signalUpdate();
		}
		return true;
	}

	public synchronized boolean update(List<ClusterInfo> clusterInfos) {
		ClusterHandlerNode newHandlerNode = new ClusterHandlerNode();
		ClusterHandlerNode oldHandler;
		newHandlerNode.setTranscoder(current.getTranscoder());
		newHandlerNode.update(clusterInfos, current);

    oldHandler = current;
		current = newHandlerNode;
    oldHandler.close();

		if (log.isDebugEnabled()) {
			log.debug("update :\n" + toString());
		}

		if (!current.canService()) {
			log.warn("NO cluster can service now.");
			return false;
		}

		return true;
	}

	public ClusterHandler pickHandler(Serializable key, int namespace) {
		return current.pickHandler(key, namespace);
	}

	public ClusterHandler[] pickAllHandler() {
		return current.pickAllHandler();
	}

	public String toString() {
		return infoUpdater.toString() + "\n" + current.toString();
	}

	public void close() {
		current.close();
		current = null;
	}
}
