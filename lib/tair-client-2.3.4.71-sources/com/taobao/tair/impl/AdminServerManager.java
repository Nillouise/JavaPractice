package com.taobao.tair.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.TairClient;
import com.taobao.tair.comm.TairClientFactory;
import com.taobao.tair.etc.TairClientException;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.PacketStreamer;
import com.taobao.tair.packet.RequestPingPacket;
import com.taobao.tair.packet.ReturnPacket;

public class AdminServerManager {
	private static final Logger log = LoggerFactory
			.getLogger(AdminServerManager.class);

	private TairClientFactory factory;
	private PacketStreamer streamer;

	private int connectTimeout = TairConstant.DEFAULT_CS_CONN_TIMEOUT;
	private int timeout = TairConstant.DEFAULT_TIMEOUT;

	private String serverAddr = null;

	public AdminServerManager(TairClientFactory factory,
			PacketStreamer streamer) {
		this.factory = factory;
		this.streamer = streamer;
	}

	public void init() {

	}

	public void updateAdminServer(Map<String, String> config) {
		if (config == null) {
			return;
		}

		serverAddr = config.get(TairConstant.ADMIN_SERVERLIST_KEY);
		if (serverAddr != null) {
      log.warn("get admin server addr: " + serverAddr);
		}
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getAddr() {
		return serverAddr;
	}

	public void close() {
	}

}
