/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.fastdump;

import java.util.List;
import java.util.HashMap;

import com.taobao.tair.etc.TairConstant;

public class ClusterInfo {
	private List<String> configServerList;
	private String groupName;
	private int failCount = 100;
	private int timeout = TairConstant.DEFAULT_TIMEOUT;
	private int compressionThreshold = TairConstant.TAIR_DEFAULT_COMPRESSION_THRESHOLD;
	private String charset = TairConstant.DEFAULT_CHARSET;
	private boolean header = true;

	public ClusterInfo() {
	}

	public ClusterInfo(ClusterInfo info) {
		this.configServerList = info.configServerList;
		this.groupName = info.groupName;
		this.failCount = info.failCount;
		this.timeout = info.timeout;
		this.compressionThreshold = info.compressionThreshold;
		this.charset = info.charset;
		this.header  = info.header;
	}

    public List<String> getConfigServerList() {
        return configServerList;
    }
    public void setConfigServerList(List<String> configServerList) {
		this.configServerList = configServerList;
	}
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	public String getGroupName() {
		return this.groupName;
	}
    public void setMaxFailCount(int failCount) {
		this.failCount = failCount;
	}
    public int getMaxFailCount() {
		return this.failCount;
	}
	public int getTimeout() {
        return timeout;
    }
    public void setTimeout(int timeout) {
		this.timeout = timeout;
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
		this.compressionThreshold = compressionThreshold;
	}
	public boolean getHeader() {
		return this.header;
	}
    public void setHeader(boolean header) {
		this.header = header;
	}

  	public boolean equals(ClusterInfo info) {
		return info.getGroupName().equals(this.getGroupName());
	}
	public String toString() {
		String configServerStr = null;
		if (configServerList == null) {
			configServerStr = "none";
		} else {
			configServerStr = new String();
			for (String configServer : configServerList) {
				configServerStr += configServer + " | ";
			}
		}
		return "[ " + configServerStr + groupName + " ]";
	}
}
