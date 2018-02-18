/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.FlowBound;
import com.taobao.tair.comm.FlowLimit.FlowType;
import com.taobao.tair.comm.FlowRate;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.RequestOpCmdPacket;
import com.taobao.tair.packet.RequestRemoveArea;
import com.taobao.tair.packet.ResponseOpCmdPacket;
import com.taobao.tair.packet.ReturnPacket;
import com.taobao.tair.packet.stat.FlowControlSet;
import com.taobao.tair.packet.stat.FlowViewRequest;
import com.taobao.tair.packet.stat.FlowViewResponse;

public class AdminTairManager extends DefaultTairManager {
	private static final Logger log = LoggerFactory.getLogger(AdminTairManager.class);

	public boolean removeNamespace(int namespace) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}
		boolean success = true;
		Set<Long> serverids = null;
		if (isDirect) {
			serverids = new HashSet<Long>();
			serverids.add(serverId);
		} else {
			serverids = super.configServer.getAliveNodes();
		}
		RequestRemoveArea request = new RequestRemoveArea(transcoder);
		request.setArea(namespace);

		for (Long id : serverids) {
			ReturnPacket ret = syncCall(id, namespace, request,
					ReturnPacket.class);
			if (null == ret) {
				log.error("remove area " + namespace + "timeout, failed ip : " + TairUtil.idToAddress(id));
				success = false;
			} else if (ret.getCode() != ResultCode.SUCCESS.getCode()) {
				log.error("remove area " + namespace + " failed ip : " + TairUtil.idToAddress(id) + " result : " + ret.getCode());
				success = false;
			} else {
				this.checkConfigVersion(ret.getConfigVersion());
			}
		}
		return success;
	}

  public Set<String> getServerAddrs(){
    Set<Long> serverids = null;
    Set<String> serverAddrs = new HashSet<String>();
    if (isDirect) {
      serverids = new HashSet<Long>();
      serverids.add(serverId);
    } else {
      serverids = super.configServer.getAliveNodes();
    }

    for (Long id : serverids) {
      serverAddrs.add(TairUtil.idToAddress(id.longValue()));
    }
    return serverAddrs;
  }

  public boolean removeNamespace(int namespace, String serverAddr) {
    if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
      throw new IllegalArgumentException("namespace illegal");
    }
    boolean success = true;
    RequestRemoveArea request = new RequestRemoveArea(transcoder);
    request.setArea(namespace);
    String address = TairUtil.getHost(serverAddr);
    int port = TairUtil.getPort(serverAddr);
    long id = TairUtil.hostToLong(address, port);
    ReturnPacket ret = syncCall(id, namespace, request, ReturnPacket.class);
	if (null == ret) {
		log.error("remove area " + namespace + "timeout, failed ip : " + serverAddr);
		success = false;
	} else if (ret.getCode() != ResultCode.SUCCESS.getCode()) {
		log.error("remove area " + namespace + " failed ip : " + serverAddr + " result : " + ret.getCode());
		success = false;
	} else {
		log.info("remove area " + namespace + " on " + serverAddr + " success.");
		this.checkConfigVersion(ret.getConfigVersion());
	}
    return success;
  }

	public int newNamespaceAuto(long quota) {
		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(TairConstant.ServerCmdType.TAIR_SERVER_CMD_ALLOC_AREA
				.ordinal());
		List<String> params = new ArrayList<String>();
		params.add(65536 + "");
		params.add(quota + "");
		params.add(groupName);
		request.setCmdParams(params);

		ResponseOpCmdPacket response = syncCall(
				TairUtil.hostToLong(configServerList.get(0)), 0, request,
				ResponseOpCmdPacket.class);
		int ret = response.getCode();
		if (ret != 0) {
			log.error("new namespace auto failed error code:" + ret);
		} else {
			List<String> rets = response.getValues();
			if (rets.size() == 1) {
				int ns = Integer.parseInt(rets.get(0));
				return ns;
			}
		}
		return -1;
	}

	public boolean newNamespace(int namespace, long quota) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}
		boolean success = true;
		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(TairConstant.ServerCmdType.TAIR_SERVER_CMD_ALLOC_AREA
				.value());
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");
		params.add(quota + "");
		params.add(groupName);
		request.setCmdParams(params);

		ResponseOpCmdPacket response = syncCall(
				TairUtil.hostToLong(configServerList.get(0)), 0, request,
				ResponseOpCmdPacket.class);
		int ret = response.getCode();
		if (ret != 0) {
			log.error("new namespace " + namespace + " failed error code:"
					+ ret);
			success = false;
		}
		return success;
	}

	public boolean deleteNamespace(int namespace) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}
		boolean success = true;
		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(TairConstant.ServerCmdType.TAIR_SERVER_CMD_SET_QUOTA
				.value());
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");
		params.add("0");
		params.add(groupName);
		request.setCmdParams(params);

		ResponseOpCmdPacket response = syncCall(
				TairUtil.hostToLong(configServerList.get(0)), 0, request,
				ResponseOpCmdPacket.class);
		int ret = response.getCode();
		if (ret != 0) {
			log.error("modfiy namespacequota " + namespace
					+ " failed error code:" + ret);
			success = false;
		}
		return success;
	}

	public boolean modifyNamespaceQuota(int namespace, long quota) {
		if ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX)) {
			throw new IllegalArgumentException("namespace illegal");
		}
		boolean success = true;
		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(TairConstant.ServerCmdType.TAIR_SERVER_CMD_SET_QUOTA
				.value());
		List<String> params = new ArrayList<String>();
		params.add(namespace + "");
		params.add(quota + "");
		params.add(groupName);
		request.setCmdParams(params);

		ResponseOpCmdPacket response = syncCall(
				TairUtil.hostToLong(configServerList.get(0)), 0, request,
				ResponseOpCmdPacket.class);
		int ret = response.getCode();
		if (ret != 0) {
			log.error("modify namespace " + namespace + " failed error code:"
					+ ret);
			success = false;
		}
		return success;
	}

	public int clearDownServers(Set<String> addrs) {
		for (String ip : addrs) {
			if (TairUtil.hostToLong(ip) == 0)
				throw new IllegalArgumentException(ip);
		}

		RequestOpCmdPacket request = new RequestOpCmdPacket(transcoder);
		request.setCmdType(TairConstant.ServerCmdType.TAIR_SERVER_CMD_FLUSH_MMT
				.value());
		List<String> params = new ArrayList<String>();
		params.add(groupName);
		params.addAll(addrs);
		request.setCmdParams(params);

		ResponseOpCmdPacket response = syncCall(
				TairUtil.hostToLong(configServerList.get(0)), 0, request,
				ResponseOpCmdPacket.class);
		return response.getCode();
	}

	public Map<String, FlowRate> viewFlowRate(int namespace) {
		if (namespace != 1024
				&& ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX))) {
			throw new IllegalArgumentException("namespace illegal");
		}
		Set<Long> serverids = super.configServer.getAliveNodes();

		Map<String, FlowRate> ret = new HashMap<String, FlowRate>();
		for (Long id : serverids) {
			ret.put(TairUtil.idToAddress(id), viewFlowRate(id, namespace));
		}

		return ret;
	}

	private FlowRate viewFlowRate(Long serverid, int namespace) {
		FlowViewRequest request = new FlowViewRequest(transcoder);
		request.setArea(namespace);

		return syncCall(serverid, namespace, request, FlowViewResponse.class)
				.getFlowrate();
	}

	public Map<String, FlowBound> setFlowBound(int namespace, FlowType type,
			FlowBound bound) {
		if (namespace != 1024
				&& ((namespace < 0) || (namespace > TairConstant.NAMESPACE_MAX))) {
			throw new IllegalArgumentException("namespace illegal");
		}
		Set<Long> serverids = super.configServer.getAliveNodes();

		Map<String, FlowBound> ret = new HashMap<String, FlowBound>();
		for (Long id : serverids) {
			ret.put(TairUtil.idToAddress(id),
					setFlowBound(id, namespace, type, bound));
		}

		return ret;
	}

	private FlowBound setFlowBound(Long serverid, int namespace, FlowType type,
			FlowBound bound) {
		FlowControlSet request = new FlowControlSet(transcoder);
		request.setNamespace(namespace);
		request.setType(type);
		request.setBound(bound);

		FlowControlSet response = syncCall(serverid, namespace, request,
				FlowControlSet.class);
		if (response.isSuccess())
			return response.getBound();
		else
			return null;
	}

	private <T extends BasePacket> T syncCall(long serverid, int namespace,
			BasePacket request, Class<T> retCls) {
		request.encode();
		BasePacket reponse = sendRequest(namespace, serverid, request, null);
		if (reponse == null)
			throw new RuntimeException("response is null");
		return retCls.cast(reponse);
	}
}
