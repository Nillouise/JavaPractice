package com.taobao.tair.extend.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.taobao.tair.ResultCode;
import com.taobao.tair.TairCallback;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.extend.packet.ResponsePacketInterface;
import com.taobao.tair.packet.BasePacket;

public interface TairManagerSession {
	
	public boolean isValidNamespace(short namespace);
	
	public boolean isVaildCount(int count);

	public <T> T sendRequest(int namespace, Object key, BasePacket packet, Class<T> cls);
	public ResultCode sendAsyncRequest(int ns, Object key, BasePacket packet, boolean isRead,
			TairCallback cb, SERVER_TYPE type);
	public ResultCode sendAsyncRequest(int ns, Long serverIp, BasePacket packet, boolean isRead,
			TairCallback cb, SERVER_TYPE type);
	
	
	public ResultCode tryEncode(BasePacket request);
	
	public void checkConfigVersion(ResponsePacketInterface response);
	
	public <T extends BasePacket> T helpNewRequest(Class<T> cls, 
			short namespace, Serializable key, short version, int expire);
	
	public <T extends BasePacket> T helpNewRequest(Class<T> cls, short namespace, int expire);
	
	public Map<Long, Set<Serializable>> classifyKeys(Collection<? extends Serializable> keys)
			throws IllegalArgumentException;
}
