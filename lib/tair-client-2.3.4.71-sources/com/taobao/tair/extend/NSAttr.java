package com.taobao.tair.extend;

public final class NSAttr {
	/**
	 * list_max_size  -->  number string
	 * hash_max_size  -->  number string
	 * zset_max_size  -->  number string
	 * set_max_size   -->  number string
	 */
	public final static NSAttr LIST_MAX_SIZE = new NSAttr("list_max_size");
	public final static NSAttr HASH_MAX_SIZE = new NSAttr("hash_max_size");
	public final static NSAttr ZSET_MAX_SIZE = new NSAttr("zset_max_size");
	public final static NSAttr SET_MAX_SIZE  = new NSAttr("set_max_size");
	
	private String attr = null;
	
	private NSAttr(String attr) {
		this.attr = attr;
	}
	
	@Override
	public String toString() {
		return attr;
	}
}
