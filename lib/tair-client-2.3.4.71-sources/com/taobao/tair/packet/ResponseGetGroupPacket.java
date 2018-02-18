/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;

public class ResponseGetGroupPacket extends BasePacket {
	
	private static final Logger log = LoggerFactory.getLogger(ResponseGetGroupPacket.class);
	 
	private int configVersion;
	private int copyCount;
	private int bucketCount;
	private List<Long> serverList;
	private Set<Long> downServers = new HashSet<Long>();
	private Map<String, String> configMap;
	private Set<Long> aliveNodes;
	

	public ResponseGetGroupPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_RESP_GET_GROUP_NEW_PACKET;
	}

	/**
	 * encode
	 */
	public int encode() {
		throw new UnsupportedOperationException();
	}
	
	private static Set<Long> parseDownServersString(String strs) {
		HashSet<Long> servers = new HashSet<Long>();
		for (String addr : strs.split(";")) {
			if (addr.trim().equals(""))
				continue;
			Long id = TairUtil.hostToLong(addr.trim());
			if (id == 0) {
				log.warn("invalid server down string " + strs);
				return null;
			}
			servers.add(id);
		}
		return servers;
	}

	/**
	 * decode
	 */
	public boolean decode() {
		this.serverList = new ArrayList<Long>();
		this.configMap = new HashMap<String, String>();

		bucketCount = byteBuffer.getInt();
		copyCount = byteBuffer.getInt();
		this.configVersion = byteBuffer.getInt();

		// get config items
		int count = byteBuffer.getInt();
		for (int i = 0; i < count; i++) {
			String name = readString();
			String value = readString();
			if (name.trim().equals(TairConstant.TAIR_TMP_DOWN_SERVER)) {
				Set<Long> ids = parseDownServersString(value.trim());
				if (ids != null) {
					downServers = ids;
				}
			}
			configMap.put(name, value);
		}

		// get server list
		count = byteBuffer.getInt();
		if (count > 0) {
			byte[] b = new byte[count];
			byteBuffer.get(b);
			byte[] result = TairUtil.deflate(b);
			ByteBuffer buff = ByteBuffer.wrap(result);
			buff.order(ByteOrder.LITTLE_ENDIAN);

			List<Long> ss = new ArrayList<Long>();
			int c = 0;
			while (buff.hasRemaining()) {
				long sid = buff.getLong();
				ss.add(sid);
				c++;
				if (c == bucketCount) {
					serverList.addAll(ss);
					ss = new ArrayList<Long>();
					c = 0;
				}
			}
		}

		aliveNodes = new HashSet<Long>();
		count = byteBuffer.getInt();
		for(int i=0; i<count; i++)
			aliveNodes.add(byteBuffer.getLong());

		return true;
	}

	/**
	 * 
	 * @return the configVersion
	 */
	public int getConfigVersion() {
		return configVersion;
	}

	/**
	 * 
	 * @param configVersion
	 *            the configVersion to set
	 */
	public void setConfigVersion(int configVersion) {
		this.configVersion = configVersion;
	}

	public List<Long> getServerList() {
		return serverList;
	}

	public Map<String, String> getConfigMap() {
		return configMap;
	}

	public int getCopyCount() {
		return copyCount;
	}

	public int getBucketCount() {
		return bucketCount;
	}

	public Set<Long> getAliveNodes() {
		return aliveNodes;
	}

	public void setAliveNodes(Set<Long> aliveNodes) {
		this.aliveNodes = aliveNodes;
	}
	
	public Set<Long> getDownServers() {
		return downServers;
	}
	
}
