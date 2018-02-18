package com.taobao.tair.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.taobao.vipserver.client.core.VIPClient;
import com.taobao.vipserver.client.core.Host;

public class InvalidServerManager {
	private static final Logger log = LoggerFactory
			.getLogger(InvalidServerManager.class);

	private InvalidServerHelper helper;

	private TairClientFactory factory;
	private PacketStreamer streamer;

	private int connectTimeout = TairConstant.DEFAULT_CS_CONN_TIMEOUT;
	private int timeout = TairConstant.DEFAULT_TIMEOUT;

	private InvalidServer[] servers = new InvalidServer[0];
	private boolean useVipServer = false;

	public InvalidServerManager(TairClientFactory factory,
			PacketStreamer streamer, String invalidServiceDomain,
			String invalidServiceCluster) {
		if (invalidServiceDomain != null && !invalidServiceDomain.isEmpty()) {
			helper = new InvalidServerHelper(invalidServiceDomain,
					invalidServiceCluster);
			this.useVipServer = true;
		}
		this.factory = factory;
		this.streamer = streamer;
	}

	public boolean init() {
		if (this.useVipServer) {
			if (helper != null) {
				// test if is working
				InvalidServer invalidServer = helper.chooseInvalidServerAuto();
				if (invalidServer == null) {
					return false;
				}
			}
		} else {
			helper = new InvalidServerHelper();
			checkInvalidServer = new CheckInvalidServer();
			checkInvalidServer.start();
		}
		return true;
	}

	public InvalidServer chooseInvalidServer() {
		if (useVipServer) {
			if (helper == null) {
				return null;
			}
			return helper.chooseInvalidServerAuto();
		} else {
			return helper.chooseInvalidServer(servers);
		}
	}

	public void updateInvalidServers(Map<String, String> config) {
		if (config == null) {
			return;
		}

		String invalidConfig = config.get(TairConstant.INVALID_SERVERLIST_KEY);
		if (invalidConfig == null) {
			servers = new InvalidServer[0];
			log.warn("no invalid server found");
			return;
		}

		Set<InvalidServer> iplist = new HashSet<InvalidServer>();
		for (String address : invalidConfig.split(",")) {
			if (address == null || "".equals(address.trim())) {
				continue;
			}
			log.info("got invalid server " + address);
			iplist.add(new InvalidServer(address.trim(), useVipServer));
		}
		servers = iplist.toArray(new InvalidServer[0]);
	}

	private class InvalidServerHelper {
		private String invalidServiceDomain;
		private String invalidServiceCluster;
		private VIPClient vipClient = new VIPClient();
		private boolean useVipServer = false;;

		private AtomicInteger lastSeq = new AtomicInteger(
				(int) Math.random() * 10);

		public InvalidServerHelper(String invalidServiceDomain, String invalidServiceCluster) {
			this.invalidServiceDomain = invalidServiceDomain;
			this.invalidServiceCluster = invalidServiceCluster;
			this.useVipServer = true;
		}

		public InvalidServerHelper() {
		}

		public InvalidServer chooseInvalidServerAuto() {
			InvalidServer invalidServer = null;
			try {
				invalidServer = new InvalidServer(vipClient.srvHost(invalidServiceDomain, invalidServiceCluster).toString(), useVipServer);
			} catch (Exception e) {
				// first call may throw Exception
				log.error("get InvalidServer from vipclient failed!");
			}
			return invalidServer;
		}

		public InvalidServer chooseInvalidServerManual() {
			List<Host> servers;
			try {
				servers = vipClient.srvHosts(invalidServiceDomain, invalidServiceCluster);
			} catch (Exception e) {
				// first call may throw Exception
				log.error("get InvalidServer list from vipclient failed!");
				return null;
			}
			for (int i = 0; i < servers.size(); ++i) {
				int seq = Math.abs(lastSeq.incrementAndGet()) % servers.size();
				InvalidServer server = new InvalidServer(servers.get(seq).toString(), useVipServer);
				return server;
			}
			return null;
		}
		public InvalidServer chooseInvalidServer(InvalidServer[] servers) {
			for (int i = 0; i < servers.length; ++i) {
				int seq = Math.abs(lastSeq.incrementAndGet()) % servers.length;
				InvalidServer server = servers[seq];
				if (server.getFailCount() < server.getMaxFailCount()) {
					return server;
				}
			}
			return null;
		}

		private boolean pingInvalidServer(String host)
				throws TairClientException {
			if (host == null) {
				return false;
			}
			TairClient client = factory.get(host, connectTimeout, timeout,
					streamer);
			RequestPingPacket request = new RequestPingPacket(null);
			BasePacket response = (BasePacket) client.invoke(0, request,
					timeout);
			if (response instanceof ReturnPacket) {
				if (((ReturnPacket) response).getCode() == ResultCode.SUCCESS
						.getCode()) {
					return true;
				}
			} else {
				log.error("response error: " + response);
			}
			return false;
		}

		public void checkInvalidServerStatus(InvalidServer[] servers) {
			for (int i = 0; i < servers.length; ++i) {
				InvalidServer server = servers[i];
				try {
					if (server.getFailCount() < server.getMaxFailCount()) {
						// nothing
					} else if (pingInvalidServer(server.getAddress())) {
						server.resetFailCount();
						log.warn("invalid server " + server.getAddress()
								+ " is revoked");
					}
				} catch (Exception e) {
					log.error("ping exception", e);
				}
			}
		}
	}

	private CheckInvalidServer checkInvalidServer = null;

	class CheckInvalidServer extends Thread {

		boolean running = true;

		private static final int SLEEPTIME = 5 * 1000; // 5s

		public CheckInvalidServer() {
			this.setDaemon(true);
		}

		public void close() {
			running = false;
		}

		@Override
		public void run() {
			while (running) {
				long start = System.currentTimeMillis();
				try {
					helper.checkInvalidServerStatus(servers);
				} catch (Exception e) {
					log.error("checkInvalidServerStatus err", e);
				}
				if (running == false)
					break;

				long spend = System.currentTimeMillis() - start;
				try {
					if (spend >= SLEEPTIME) {
						Thread.sleep(500);
					} else {
						Thread.sleep(SLEEPTIME - spend);
					}
				} catch (InterruptedException e) {
					if (running == false) {
						break;
					}
					log.warn("can't be interrupt", e);
				}
			}
		}

	};

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void close() {
		if (null != checkInvalidServer) {
			checkInvalidServer.close();
			checkInvalidServer.interrupt();
		}
	}

	public boolean isUseVipServer() {
		return useVipServer;
	}
}
