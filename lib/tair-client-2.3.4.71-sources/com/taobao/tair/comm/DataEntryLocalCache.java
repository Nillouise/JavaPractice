package com.taobao.tair.comm;


public class DataEntryLocalCache extends LocalCache<Object, CacheEntry> {

	public DataEntryLocalCache(String id, ClassLoader customClassLoader) {
		super(id, CacheEntry.class, customClassLoader);
	}
}
