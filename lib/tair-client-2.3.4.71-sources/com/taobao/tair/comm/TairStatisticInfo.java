package com.taobao.tair.comm;

import com.taobao.tair.StatisticInfo;

public class TairStatisticInfo extends StatisticInfo {
	private String groupName = null;
	private int ns = 0;
	private int rc = -1;
	public String getGroupName() {
		return groupName;
	}
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	public int getNs() {
		return ns;
	}
	public void setNs(int ns) {
		this.ns = ns;
	}
	public int getRc() {
		return rc;
	}
	public void setRc(int rc) {
		this.rc = rc;
	}
}
