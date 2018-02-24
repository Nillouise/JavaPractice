/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.fastdump;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.impl.DefaultTairManager;
import com.taobao.tair.ResultCode;
import com.taobao.tair.etc.TairConstant;

class ClusterHandler {
	private static final Logger log = LoggerFactory.getLogger(ClusterHandler.class);

	private ClusterInfo clusterInfo;
	private DefaultTairManager tairManager = new DefaultTairManager();
	// DefaultTairManager can update version automatically, we may ignore
	// cluster change if just check version by getConfigVersion() before/after
	// one fuction operation(get()/setSync() etc.), 'cause getConfigVersion() may just
	// happen on version-changing process. So, ClusterHandler save lastVersion to
	// be a check base version.
	private int lastVersion = 0;
	private int index = 0;
	private List<Long> downServers = new ArrayList<Long>();
	private Set<Integer> downBuckets = new HashSet<Integer>();
	private Map<Integer, Integer> namespaceStatusMap = new HashMap<Integer, Integer>();

  public void setNsMapStatus(int namespace, int status){
    namespaceStatusMap.put(namespace, status);
  }

  public int getNsMapStatus(int namespace){
    Integer ret = namespaceStatusMap.get(namespace);
    if (ret != null){
      return ret;
    }else{
      return TairConstant.TAIR_NS_STATUS_INACTIVE;
    }
  }

  public Map<Integer, Integer>getNsMap(){
    return namespaceStatusMap;
  }

	public List<Long> getDownServers() {
		return downServers;
	}

	public boolean hasDownAreas() {
    for (Iterator iter = namespaceStatusMap.entrySet().iterator(); iter.hasNext();){
      Map.Entry entry = (Map.Entry)iter.next();
      if ((Integer)entry.getValue() == TairConstant.TAIR_NS_STATUS_OFF)  {
        return true;
      }
    }
		return false;
	}

	public Set<Integer> getDownBuckets() {
		return downBuckets;
	}

	public void addDownServer(long serverId) {
		downServers.add(serverId);
	}

	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

	public ClusterInfo getClusterInfo() {
		return this.clusterInfo;
	}

	public void setTairManager(DefaultTairManager tairManager) {
		this.tairManager = tairManager;
	}

	public DefaultTairManager getTairManager() {
		return this.tairManager;
	}

	public int getLastVersion() {
		return lastVersion;
	}

	public void setLastVersion(int lastVersion) {
		this.lastVersion = lastVersion;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getConfigVersion() {
		return tairManager.getConfigVersion();
	}

	public int getBucketCount() {
		return tairManager.getBucketCount();
	}

	public Map<String, String> retrieveConfigMap() {
		return tairManager.retrieveConfigMap();
	}

  public List<String> getNsStatus(List<String> groups) {
		return tairManager.getNsStatus(groups);
	}
  public List<String> getGroupStatus(List<String> groups) {
		return tairManager.getGroupStatus(groups);
	}
	public List<Integer> getBucketByServer(long serverId) {
		return tairManager.getBucketByServer(serverId);
	}

	public void reset() {
		downServers.clear();
		downBuckets.clear();
	}

	public boolean init() {
		try {
			tairManager.setConfigServerList(clusterInfo.getConfigServerList());
			tairManager.setGroupName(clusterInfo.getGroupName());
			tairManager.setTimeout(clusterInfo.getTimeout());
			tairManager.setMaxFailCount(clusterInfo.getMaxFailCount());
			tairManager.setCompressionThreshold(clusterInfo.getCompressionThreshold());
			tairManager.setCharset(clusterInfo.getCharset());
			tairManager.setHeader(clusterInfo.getHeader());
			tairManager.init();
			this.lastVersion = tairManager.getConfigVersion();
		} catch (RuntimeException e) {
			log.error("init tairmanager fail: "	+ clusterInfo);
			return false;
		}
		return true;
	}

  public void close(){
    tairManager.close();
  }

	public String toString() {
		return clusterInfo.toString();
	}

}
