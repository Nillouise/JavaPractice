/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.fastdump;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.ResultCode;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;

public class ClusterInfoUpdater extends Thread {

	class UpdateResult {
		private ResultCode resultCode;
		private boolean urgent;

		public UpdateResult(ResultCode resultCode, boolean urgent) {
			this.resultCode = resultCode;
			this.urgent = urgent;
		}
		public boolean isSuccess() {
			return resultCode.getCode() == ResultCode.SUCCESS.getCode();
		}
		public boolean isUrgent() {
			return urgent;
		}
		public void setResultCode(ResultCode resultCode) {
			this.resultCode = resultCode;
		}
		public ResultCode getResultCode() {
			return resultCode;
		}
		public void setUrgent(boolean urgent) {
			this.urgent = urgent;
		}

	}


	private static final Logger log = LoggerFactory.getLogger(ClusterInfoUpdater.class);

	private static final int DEFAULT_UPDATE_CLUSTER_INFO_INTERVAL_MS = 60000; // 1min
	private static final int FAIL_UPDATE_CLUSTER_INFO_INTERVAL_MS = 2000; // 2s
	private static final int URGENT_UPDATE_CLUSTER_INFO_INTERVAL_MS = 4000; // 4s

	private ClusterHandler masterHandler = new ClusterHandler();
	private ClusterHandlerManager handlerManager;
	private int updateIntervalMs = DEFAULT_UPDATE_CLUSTER_INFO_INTERVAL_MS;
	private boolean running = true;
	private Integer updater = new Integer(0);
	private long lastUpdateTime = 0;
	private boolean doForceUpdate = false;

	public ClusterInfoUpdater() {
		setDaemon(true);
	}

  public void setUpdaterInterval(int milliseconds) {
    this.updateIntervalMs = milliseconds; 
  }

	public void setClusterInfo(ClusterInfo clusterInfo) {
		masterHandler.setClusterInfo(clusterInfo);
	}

	public ClusterInfo getClusterInfo() {
		return masterHandler.getClusterInfo();
	}

	public ClusterHandler getMasterClusterHandler() {
		return masterHandler;
	}

	public void setClusterHandlerManager(ClusterHandlerManager handlerManager) {
		this.handlerManager = handlerManager;
	}

	public ClusterHandlerManager getClusterHandlerManager() {
		return this.handlerManager;
	}

	public boolean forceUpdate() {
		return updateClusterInfo(true).isSuccess();
	}

	public boolean signalUpdate() {
		log.debug("signal update");
		synchronized(updater) {
			doForceUpdate = true;
			updater.notify();
		}
		return true;
	}

	public void close() {
		running = false;
		synchronized(updater) {
			updater.notifyAll();
		}
	}

	public boolean init() {
		masterHandler.getTairManager().setForceService(true);
		return masterHandler.init();
	}

	public void run() {
		int eachUpdateInterval = this.updateIntervalMs;
		UpdateResult updateResult;

		while (running) {
			try {
				log.debug("do update " + doForceUpdate);
				// do update
				updateResult = updateClusterInfo(doForceUpdate);
				synchronized(updater) {
					// not do force update in default condition
					doForceUpdate = false;

					if (!updateResult.isSuccess()) {
						eachUpdateInterval = FAIL_UPDATE_CLUSTER_INFO_INTERVAL_MS;
						log.error("update cluster info fail. will retry after " + eachUpdateInterval + " ms");
					} else if (updateResult.isUrgent()) {
						eachUpdateInterval = URGENT_UPDATE_CLUSTER_INFO_INTERVAL_MS;
						log.error("urgent condition, maybe all cluster are dead. will retry after " + eachUpdateInterval);
					} else {
						eachUpdateInterval = this.updateIntervalMs;
						log.debug("update " + eachUpdateInterval);
					}

					updater.wait(eachUpdateInterval);
				}
			} catch (InterruptedException e) {
				log.error("clusterInfoUpdate interrupt when wait: " + e);
			}
		}
	}

	public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "[ updater version: " + masterHandler.getConfigVersion() + ", last update time: " +
			sdf.format(new Date(lastUpdateTime)) + " ]";
	}

	private UpdateResult updateClusterInfo(boolean force) {
		int oldVersion = masterHandler.getConfigVersion();
		// get config map
		Map<String, String> configMap = masterHandler.retrieveConfigMap();
		if (configMap == null) {
			log.error("update cluster info fail: get null configmap: " + masterHandler);
			return new UpdateResult(ResultCode.SERVERERROR, true);
		}
		int newVersion = masterHandler.getConfigVersion();

		// no need update
		if (!force && newVersion == oldVersion) {
			log.debug("no need update " + newVersion + " <> " + oldVersion);
			return new UpdateResult(ResultCode.SUCCESS, false);
		}

		List<String> clusterInfoConfig = TairUtil.parseConfig(configMap, TairConstant.TAIR_MULTI_GROUPS,
														TairConstant.TAIR_CONFIG_VALUE_DELIMITERS);
		if (clusterInfoConfig == null || clusterInfoConfig.isEmpty()) {
			log.error("update cluster info fail: NO cluster info config: " + masterHandler);
			return new UpdateResult(ResultCode.SERVERERROR, false);
		}

		List<ClusterInfo> clusterInfos = new ArrayList<ClusterInfo>(clusterInfoConfig.size());
		for (String config : clusterInfoConfig) {
			// we use one configserver now. only group
			ClusterInfo info = new ClusterInfo(masterHandler.getClusterInfo());
			info.setGroupName(config);
			clusterInfos.add(info);
		}

		boolean ret = handlerManager.update(clusterInfos);
		lastUpdateTime = System.currentTimeMillis();
		// if fail, we need an urgent update
		return new UpdateResult(ResultCode.SUCCESS, !ret);
	}

}
