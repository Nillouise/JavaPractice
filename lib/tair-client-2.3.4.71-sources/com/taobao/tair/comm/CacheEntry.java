package com.taobao.tair.comm;

import java.io.Serializable;

import com.taobao.tair.DataEntry;

public class CacheEntry implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public CacheEntry(DataEntry data, Status status) {
		super();
		this.data = data;
		this.status = status;
	}
	
	public DataEntry data;
	public Status status;
	public static enum Status { EXIST, NOTEXIST, DELEDTED, DIRTY };
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("DataEntry: ").append(data.toString());
		sb.append("Status: ").append(status);
		return sb.toString();
	}
}
