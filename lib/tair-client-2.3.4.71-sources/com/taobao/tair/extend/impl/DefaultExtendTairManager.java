/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.extend.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairCallback;
import com.taobao.tair.TairManager;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairSendRequestStatus;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.etc.TairConstant.EngineType;
import com.taobao.tair.extend.DataEntryDouble;
import com.taobao.tair.extend.DataEntryList;
import com.taobao.tair.extend.DataEntryLong;
import com.taobao.tair.extend.DataEntryMap;
import com.taobao.tair.extend.DataEntrySet;
import com.taobao.tair.extend.DataEntrySimple;
import com.taobao.tair.extend.DataEntryType;
import com.taobao.tair.extend.NSAttr;
import com.taobao.tair.extend.TairManagerCommon;
import com.taobao.tair.extend.TairManagerHset;
import com.taobao.tair.extend.TairManagerList;
import com.taobao.tair.extend.TairManagerSet;
import com.taobao.tair.extend.TairManagerZset;
import com.taobao.tair.extend.packet.ResponsePacketInterface;
import com.taobao.tair.extend.packet.common.request.RequestAddFilterPacket;
import com.taobao.tair.extend.packet.common.request.RequestDumpAreaPacket;
import com.taobao.tair.extend.packet.common.request.RequestExistsPacket;
import com.taobao.tair.extend.packet.common.request.RequestExpirePacket;
import com.taobao.tair.extend.packet.common.request.RequestLoadAreaPacket;
import com.taobao.tair.extend.packet.common.request.RequestRemoveFilterPacket;
import com.taobao.tair.extend.packet.common.request.RequestTTLPacket;
import com.taobao.tair.extend.packet.common.request.RequestTypePacket;
import com.taobao.tair.extend.packet.common.response.ResponseExpirePacket;
import com.taobao.tair.extend.packet.common.response.ResponseTTLPacket;
import com.taobao.tair.extend.packet.common.response.ResponseTypePacket;
import com.taobao.tair.extend.packet.string.request.RequestGetSetPacket;
import com.taobao.tair.extend.packet.string.request.RequestPutnxPacket;
import com.taobao.tair.extend.packet.string.response.ResponseGetSetPacket;
import com.taobao.tair.impl.DefaultTairManager;
import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.ReturnPacket;

public class DefaultExtendTairManager extends DefaultTairManager implements TairManager, 
		TairManagerCommon,
		TairManagerList,
		TairManagerHset,
		TairManagerSet,
		TairManagerZset {
	private static final Logger log = LoggerFactory.getLogger(DefaultExtendTairManager.class);
	
	private TairManagerList listImpl = null;
	private TairManagerHset hsetImpl = null;
	private TairManagerSet  setImpl = null;
	private TairManagerZset zsetImpl = null;
	
	private static boolean isValidNamespace(short namespace) {
		return TairUtil.isValidNamespace(namespace);
	}
	
	public DefaultExtendTairManager() {
		super();
		this.engineType = EngineType.RDB;
	}

	public DefaultExtendTairManager(String name, boolean sharedFactory, int processorCount) {
		super(name, sharedFactory, processorCount);
		this.engineType = EngineType.RDB;
	}
	
	public DefaultExtendTairManager(String name, boolean sharedFactory) {
		super(name, sharedFactory);
		this.engineType = EngineType.RDB;
	}

	@Override
	public void init() {
		TairManagerSession session = new TairManagerSession() {
			
			public <T> T sendRequest(int namespace, Object key, BasePacket packet, Class<T> cls) {
				return DefaultExtendTairManager.this.sendRequest(namespace, key, packet, cls);
			}

			public void checkConfigVersion(ResponsePacketInterface response) {
				DefaultExtendTairManager.this.checkConfigVersion(response);
			}

			public <T extends BasePacket> T helpNewRequest(Class<T> cls,
					short namespace, Serializable key, short version, int expire) {
				return DefaultExtendTairManager.this.helpNewRequest(cls, 
						namespace, key, version, expire);
			}
			
			public ResultCode tryEncode(BasePacket request) {
				return DefaultExtendTairManager.this.tryEncode(request);
			}
			
			public boolean isValidNamespace(short namespace) {
				return DefaultExtendTairManager.isValidNamespace(namespace);
			}
			
			public boolean isVaildCount(int count) {
				return DefaultExtendTairManager.isVaildCount(count);
			}

			public ResultCode sendAsyncRequest(int ns, Object key, BasePacket packet,
					boolean isRead, TairCallback cb, SERVER_TYPE type) {
				return DefaultExtendTairManager.this.sendAsyncRequest(ns, key, packet,
						isRead, cb, type, null);
			}
			
			public ResultCode sendAsyncRequest(int ns, Long serverIp, BasePacket packet,
					boolean isRead, TairCallback cb, SERVER_TYPE type) {
				return DefaultExtendTairManager.this.sendAsyncRequest(ns, serverIp, packet,
						isRead, cb, type, null);
			}
			
			public Map<Long, Set<Serializable>> classifyKeys(Collection<? extends Serializable> keys)
					throws IllegalArgumentException{
				return DefaultExtendTairManager.this.classifyKeys(keys);
			}

			public <T extends BasePacket> T helpNewRequest(
					Class<T> cls, short namespace, int expire) {
				return DefaultExtendTairManager.this.helpNewRequest(cls, namespace,
						expire);
			}
		};
		
		listImpl = new TairListImpl(session);
		hsetImpl = new TairHsetImpl(session);
		setImpl = new TairSetImpl(session);
		zsetImpl = new TairZsetImpl(session);
		super.init();
	}
	
	private static boolean isVaildCount(int count) {
		if (count > 0) {
			return true;
		}
		return false;
	}

	private <T> T sendRequest(int namespace, Object key, BasePacket packet, Class<T> cls) {
		BasePacket resp = sendRequest(namespace, key, packet, false, null);
		if (resp == null) {
			return null;
		}
		
		try {
			return cls.cast(resp);
		} catch (ClassCastException e) {
			log.error("Response Class (" + resp.getClass().toString() + ") is not " + cls.getName());
		}
		return null;
	}
	
	private void checkConfigVersion(ResponsePacketInterface response) {
		if (!this.isDirect)
			configServer.checkConfigVersion(response.getConfigVersion());
	}
	
	private ResultCode tryEncode(BasePacket request) {
		int code = request.encode();
		if (code == TairConstant.KEYTOLARGE) {
			return ResultCode.KEYTOLARGE;
		} else if (code == TairConstant.VALUETOLARGE) {
			return ResultCode.VALUETOLARGE;
		} else if (code == TairConstant.SERIALIZEERROR) {
			return ResultCode.SERIALIZEERROR;
		} else if (code == TairConstant.DATALENTOOLONG) {
			return ResultCode.TAIR_DATA_LEN_LIMIT;
		}
		return ResultCode.SUCCESS;
	}
	
	private <T extends BasePacket> T helpNewRequest(Class<T> cls, 
			short namespace, int expire) {
		T request = null;
		try {
			request = cls.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		request.setTranscode(transcoder);
		request.setNamespace(namespace);
		request.setExpire(expire);
		return request;
	}

	private <T extends BasePacket> T helpNewRequest(Class<T> cls, 
			short namespace) {
		T request = null;
		try {
			request = cls.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		request.setTranscode(transcoder);
		request.setNamespace(namespace);
		return request;
	}
	
	private <T extends BasePacket> T helpNewRequest(Class<T> cls, 
			short namespace, Serializable key, 
			short version, int expire) {
		T request = null;
		try {
			request = cls.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		request.setTranscode(transcoder);
		request.setKey(key);
		request.setNamespace(namespace);
		request.setVersion(version);
		request.setExpire(expire);
		return request;
	}
//***********************************API Implements**************************//
	
//************************************String API*****************************//
	//new expire strategy is not as before，so need override
	public ResultCode put(int namespace, Serializable key, Serializable value) {
		return super.put(namespace, key, value, TairConstant.NOT_CARE_VERSION,
				TairConstant.NOT_CARE_EXPIRE);
	}

	public ResultCode put(int namespace, Serializable key, Serializable value,
						  int version) {
		return super.put(namespace, key, value, version,
				TairConstant.NOT_CARE_EXPIRE);
	}
	
	public ResultCode put(int namespace, Serializable key, Serializable value,
			int version, int expire) {
		expire = TairUtil.getDuration(expire);
		return super.put(namespace, key, value, version, expire);
	}
	
	public ResultCode putAsync(int namespace, Serializable key, Serializable value,
			int version, int expire, boolean fillCache, TairCallback cb) {
		expire = TairUtil.getDuration(expire);
		return super.putAsync(namespace, key, value, version, expire, fillCache, cb);
	}
	
	public ResultCode putAsync(int namespace, Serializable key,
			Serializable value) {
		return super.putAsync(namespace, key, value, TairConstant.NOT_CARE_VERSION,
				TairConstant.NOT_CARE_EXPIRE, false, null);
	}
	
	public ResultCode putAsync(int namespace, Serializable key,
			Serializable value, TairCallback cb) {
		return super.putAsync(namespace, key, value, TairConstant.NOT_CARE_VERSION,
				TairConstant.NOT_CARE_EXPIRE, false, cb);
	}
	
	public ResultCode putnx(short namespace, Serializable key, Serializable value,
			short version, int expire) {
		if (isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestPutnxPacket request = helpNewRequest(RequestPutnxPacket.class, 
				namespace, key, version, expire);
		request.setData(value);
		
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		ReturnPacket response = sendRequest(namespace, key, request, ReturnPacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		} 
		checkConfigVersion(response);
		
		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode setCountRdb(int namespace, Serializable key, int count) {
		return super.setCount(namespace, key, count, 0, 0, true);
	}

	public ResultCode setCountRdb(int namespace, Serializable key, int count,int version, int expireTime) {
		return super.setCount(namespace, key, count, version, expireTime, true);
	}
	
	public Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime) {
		expireTime = TairUtil.getDuration(expireTime);
		return super.incr(namespace, key, value, defaultValue, expireTime);
	}
	
	public Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime) {
		expireTime = TairUtil.getDuration(expireTime);
		return super.decr(namespace, key, value, defaultValue, expireTime);
	}
	
	public Result<DataEntry> getset(short namespace, Serializable key, Serializable value,
			short version, int expire) {
		if (isValidNamespace(namespace) == false) {
			return new Result<DataEntry>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestGetSetPacket request = helpNewRequest(RequestGetSetPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntry>(resCode);

		ResponseGetSetPacket response = sendRequest(namespace, key, request, ResponseGetSetPacket.class);
		if (response == null) {
			return new Result<DataEntry>(ResultCode.CONNERROR);
		} 
		checkConfigVersion(response);
		
		return new Result<DataEntry>(ResultCode.valueOf(response.getResultCode()),
				new DataEntry(key, response.getValue(), response.getVersion()));
	}
	
//**************************************List API*****************************//
	public Result<DataEntryLong> llen(short namespace, Serializable key) {
		return listImpl.llen(namespace, key);
	}

	public Result<DataEntryLong> lrem(short namespace, Serializable key,
			Serializable value, int count, short version, int expire) {
		return listImpl.lrem(namespace, key, value, count, version, expire);
	}

	public ResultCode ltrim(short namespace, Serializable key, 
			int start, int end, short version, int expire) {
		return listImpl.ltrim(namespace, key, start, end, version, expire);
	}

	public Result<DataEntrySimple> lindex(short namespace, Serializable key, int index) {		
		return listImpl.lindex(namespace, key, index);
	}
	
	public Result<DataEntryList> lrange(short namespace, Serializable key,
			int start, int end) {
		return listImpl.lrange(namespace, key, start, end);
	}
	
	public Result<DataEntryLong> rpush(short namespace, 
			Serializable key, Serializable val, 
			short version, int expire) {
		return listImpl.rpush(namespace, key, val, version, expire);
	}
	
	public Result<DataEntryLong> rpush(short namespace, 
			Serializable key, List<? extends Serializable> vals, 
			short version, int expire) {
		return listImpl.rpush(namespace, key, vals, version, expire);
	}

	public Result<DataEntryLong> lpush(short namespace,  
			Serializable key, Serializable value, 
			short version, int expire) {
		return listImpl.lpush(namespace, key, value, version, expire);
	}
	
	public Result<DataEntryLong> lpush(short namespace, 
			Serializable key, List<? extends Serializable> vals, 
			short version, int expire) {
		return listImpl.lpush(namespace, key, vals, version, expire);
	}

	public Result<DataEntryList> lpop(short namespace, Serializable key,
			int count, short version, int expire) {
		return listImpl.lpop(namespace, key, count, version, expire);
	}
	
	public Result<DataEntryList> rpop(short namespace, Serializable key,
			int count, short version, int expire) {
		return listImpl.rpop(namespace, key, count, version, expire);
	}
	

	public ResultCode lremAsync(short namespace, Serializable key,
			Serializable value, int count, short version, int expire) {
		return listImpl.lremAsync(namespace, key, value, count, version, expire, null);
	}

	public ResultCode lremAsync(short namespace, Serializable key,
			Serializable value, int count, short version, int expire, TairCallback cb) {
		return listImpl.lremAsync(namespace, key, value, count, version, expire, cb);
	}

	public ResultCode rpopAsync(short namespace, Serializable key,
			int count, short version, int expire) {
		return listImpl.rpopAsync(namespace, key, count, version, expire, null);
	}

	public ResultCode rpopAsync(short namespace, Serializable key,
			int count, short version, int expire, TairCallback cb) {
		return listImpl.rpopAsync(namespace, key, count, version, expire, cb);
	}

	public ResultCode lpopAsync(short namespace, Serializable key,
			int count, short version, int expire) {
		return listImpl.lpopAsync(namespace, key, count, version, expire, null);
	}

	public ResultCode lpopAsync(short namespace, Serializable key,
			int count, short version, int expire, TairCallback cb) {
		return listImpl.lpopAsync(namespace, key, count, version, expire, cb);
	}

	public ResultCode rpushAsync(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		return listImpl.rpushAsync(namespace, key, value, version, expire, null);
	}

	public ResultCode rpushAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, TairCallback cb) {
		return listImpl.rpushAsync(namespace, key, value, version, expire, cb);
	}

	public ResultCode rpushAsync(short namespace, Serializable key,
			List<? extends Serializable> vals, short version, int expire) {
		return listImpl.rpushAsync(namespace, key, vals, version, expire, null);
	}

	public ResultCode rpushAsync(short namespace, Serializable key,
			List<? extends Serializable> vals, short version, int expire, TairCallback cb) {
		return listImpl.rpushAsync(namespace, key, vals, version, expire, cb);
	}

	public ResultCode lpushAsync(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		return listImpl.lpushAsync(namespace, key, value, version, expire, null);
	}

	public ResultCode lpushAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, TairCallback cb) {
		return listImpl.lpushAsync(namespace, key, value, version, expire, cb);
	}

	public ResultCode lpushAsync(short namespace, Serializable key,
			List<? extends Serializable> vals, short version, int expire) {
		return listImpl.lpushAsync(namespace, key, vals, version, expire, null);
	}

	public ResultCode lpushAsync(short namespace, Serializable key,
			List<? extends Serializable> vals, short version, int expire, TairCallback cb) {
		return listImpl.lpushAsync(namespace, key, vals, version, expire, cb);
	}


	public Result<DataEntryLong> rpushLimit(short namespace, Serializable key,
			Serializable value, int maxcount, short version, int expire) {
		return listImpl.rpushLimit(namespace, key, value, maxcount, version, expire);
	}

	public Result<DataEntryLong> rpushLimit(short namespace, Serializable key,
			List<? extends Serializable> vals, int maxcount, short version,
			int expire) {
		return listImpl.rpushLimit(namespace, key, vals, maxcount, version, expire);
	}

	public Result<DataEntryLong> lpushLimit(short namespace, Serializable key,
			Serializable value, int maxcount, short version, int expire) {
		return listImpl.lpushLimit(namespace, key, value, maxcount, version, expire);
	}

	public Result<DataEntryLong> lpushLimit(short namespace, Serializable key,
			List<? extends Serializable> vals, int maxcount, short version,
			int expire) {
		return listImpl.lpushLimit(namespace, key, vals, maxcount, version, expire);
	}

	public ResultCode ltrimAsync(short namespace, Serializable key, int start,
			int end, short version, int expire) {
		return listImpl.ltrimAsync(namespace, key, start, end, version, expire, null);
	}

	public ResultCode ltrimAsync(short namespace, Serializable key, int start,
			int end, short version, int expire, TairCallback cb) {
		return listImpl.ltrimAsync(namespace, key, start, end, version, expire, cb);
	}
//=====================end list api=======================//
	
	
//***********************************HSET API*********************************************//

	public ResultCode hexists(short namespace, Serializable key,
			Serializable field) {
		return hsetImpl.hexists(namespace, key, field);
	}
	
	public Result<DataEntryMap> hgetall(short namespace, Serializable key) {
		return hsetImpl.hgetall(namespace, key);
	}

	public Result<DataEntryLong> hincrby(short namespace, Serializable key, 
			Serializable field, int addvalue, short version, int expire) {
		return hsetImpl.hincrby(namespace, key, field, addvalue, version, expire);
	}

	public Result<DataEntryMap> hmset(short namespace, Serializable key,
			Map<? extends Serializable, ? extends Serializable> field_values,
			 short version, int expire) {
		return hsetImpl.hmset(namespace, key, field_values, version, expire);
	}
	
	public ResultCode hset(short namespace, Serializable key, Serializable field,
			Serializable value, short version, int expire) {
		return hsetImpl.hset(namespace, key, field, value, version, expire);
	}
	
	public Result<DataEntrySimple> hget(short namespace, Serializable key,
			Serializable field) {
		return hsetImpl.hget(namespace, key, field);
	}

	public Result<DataEntryMap> hmget(short namespace, Serializable key,
			List<? extends Serializable> fields) {
		return hsetImpl.hmget(namespace, key, fields);
	}

	public Result<DataEntryList> hvals(short namespace, Serializable key) {
		return hsetImpl.hvals(namespace, key);
	}

	public Result<DataEntryLong> hlen(short namespace, Serializable key) {
		return hsetImpl.hlen(namespace, key);
	}

	public ResultCode hdel(short namespace, Serializable key,
			Serializable field, short version, int expire) {
		return hsetImpl.hdel(namespace, key, field, version, expire);
	}

	public ResultCode hincrbyAsync(short namespace,
			Serializable key, Serializable field, int addvalue, short version,
			int expire) {
		return hsetImpl.hincrbyAsync(namespace, key, field, addvalue, version, expire, null);
	}

	public ResultCode hincrbyAsync(short namespace,
			Serializable key, Serializable field, int addvalue, short version,
			int expire, TairCallback cb) {
		return hsetImpl.hincrbyAsync(namespace, key, field, addvalue, version, expire, cb);
	}

	public ResultCode hmsetAsync(short namespace, Serializable key,
			Map<? extends Serializable, ? extends Serializable> field_values,
			short version, int expire) {
		return hsetImpl.hmsetAsync(namespace, key, field_values, version, expire, null);
	}

	public ResultCode hmsetAsync(short namespace, Serializable key,
			Map<? extends Serializable, ? extends Serializable> field_values,
			short version, int expire, TairCallback cb) {
		return hsetImpl.hmsetAsync(namespace, key, field_values, version, expire, cb);
	}

	public ResultCode hsetAsync(short namespace, Serializable key,
			Serializable field, Serializable value, short version, int expire) {
		return hsetImpl.hsetAsync(namespace, key, field, value, version, expire, null);
	}

	public ResultCode hsetAsync(short namespace, Serializable key,
			Serializable field, Serializable value, short version, int expire,
			TairCallback cb) {
		return hsetImpl.hsetAsync(namespace, key, field, value, version, expire, cb);
	}

	public ResultCode hdelAsync(short namespace, Serializable key,
			Serializable field, short version, int expire) {
		return hsetImpl.hdelAsync(namespace, key, field, version, expire, null);
	}

	public ResultCode hdelAsync(short namespace, Serializable key,
			Serializable field, short version, int expire, TairCallback cb) {
		return hsetImpl.hdelAsync(namespace, key, field, version, expire, cb);
	}
	
//**********************************ZSET API*********************************************//
	public Result<DataEntryDouble> zscore(short namespace, Serializable key,
			Serializable value) {
		return zsetImpl.zscore(namespace, key, value);
	}

	public Result<DataEntryList> zrange(short namespace, Serializable key,
			int start, int end, boolean withscore) {
		return zsetImpl.zrange(namespace, key, start, end, withscore);
	}

	public Result<DataEntryList> zrevrange(short namespace, Serializable key,
			int start, int end, boolean withscore) {
		return zsetImpl.zrevrange(namespace, key, start, end, withscore);
	}

	public Result<DataEntryList> zrangebyscore(short namespace,
			Serializable key, double start, double end) {
		return zsetImpl.zrangebyscore(namespace, key, start, end);
	}

	public Result<DataEntryList> zrangebyscore(short namespace,
			Serializable key, double min, double max, int limit,
			boolean withscore) {
		return zsetImpl.zrangebyscore(namespace, key, min, max, limit, withscore);
	}

	public Result<DataEntryList> zrevrangebyscore(short namespace,
			Serializable key, double max, double min, int limit,
			boolean withscore) {
		return zsetImpl.zrevrangebyscore(namespace, key, max, min, limit, withscore);
	}
	
	public ResultCode zadd(short namespace, Serializable key,
			Serializable value, double score, short version, int expire) {
		return zsetImpl.zadd(namespace, key, value, score, version, expire);
	}
	
	public Result<DataEntryLong> zrank(short namespace, Serializable key,
			Serializable value) {
		return zsetImpl.zrank(namespace, key, value);
	}

	public Result<DataEntryLong> zcard(short namespace, Serializable key) {
		return zsetImpl.zcard(namespace, key);
	}
	
	public Result<DataEntryLong> zrevrank(short namespace, Serializable key,
			Serializable value) {
		return zsetImpl.zrevrank(namespace, key, value);
	}

	public ResultCode zrem(short namespace, Serializable key, Serializable value,
			short version, int expire) {
		return zsetImpl.zrem(namespace, key, value, version, expire);
	}

	public Result<DataEntryLong> zremrangebyscore(short namespace, Serializable key,
			double start, double end, short version, int expire) {
		return zsetImpl.zremrangebyscore(namespace, key, start, end, version, expire);
	}

	public Result<DataEntryLong> zremrangebyrank(short namespace, Serializable key,
			int start, int end, short version, int expire) {
		return zsetImpl.zremrangebyrank(namespace, key, start, end, version, expire);
	}

	public Result<DataEntryLong> zcount(short namespace, Serializable key,
			double start, double end) {
		return zsetImpl.zcount(namespace, key, start, end);
	}
	
	public Result<DataEntryDouble> zincrby(short namespace, Serializable key,
			Serializable value, double addvalue, short version, int expire) {
		return zsetImpl.zincrby(namespace, key, value, addvalue, version, expire);
	}

	public ResultCode zaddAsync(short namespace, Serializable key,
			Serializable value, double score, short version, int expire) {
		return zsetImpl.zaddAsync(namespace, key, value, score, version, expire, null);
	}

	public ResultCode zaddAsync(short namespace, Serializable key,
			Serializable value, double score, short version, int expire,
			TairCallback cb) {
		return zsetImpl.zaddAsync(namespace, key, value, score, version, expire, cb);
	}

	public ResultCode zremAsync(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		return zsetImpl.zremAsync(namespace, key, value, version, expire, null);
	}

	public ResultCode zremAsync(short namespace, Serializable key,
			Serializable value, short version, int expire,
			TairCallback cb) {
		return zsetImpl.zremAsync(namespace, key, value, version, expire, cb);
	}

	public ResultCode zremrangebyscoreAsync(short namespace,
			Serializable key, double start, double end,
			short version, int expire) {
		return zsetImpl.zremrangebyscoreAsync(namespace, key, start,
				end, version, expire, null);
	}

	public ResultCode zremrangebyscoreAsync(short namespace,
			Serializable key, double start, double end,
			short version, int expire, TairCallback cb) {
		return zsetImpl.zremrangebyscoreAsync(namespace, key, start,
				end, version, expire, cb);
	}

	public ResultCode zremrangebyrankAsync(short namespace,
			Serializable key, int start, int end,
			short version, int expire) {
		return zsetImpl.zremrangebyrankAsync(namespace, key, start,
				end, version, expire, null);
	}

	public ResultCode zremrangebyrankAsync(short namespace,
			Serializable key, int start, int end,
			short version, int expire, TairCallback cb) {
		return zsetImpl.zremrangebyrankAsync(namespace, key, start,
				end, version, expire, cb);
	}

	public ResultCode zincrbyAsync(short namespace,
			Serializable key, Serializable value, double addvalue,
			short version, int expire) {
		return zsetImpl.zincrbyAsync(namespace, key, value, addvalue, version, expire, null);
	}

	public ResultCode zincrbyAsync(short namespace,
			Serializable key, Serializable value, double addvalue,
			short version, int expire, TairCallback cb) {
		return zsetImpl.zincrbyAsync(namespace, key, value, addvalue, version, expire, cb);
	}
	
//***********************************SET API*********************************************//
	public ResultCode sadd(short namespace, Serializable key, Serializable value,
			short version, int expire) {
		return setImpl.sadd(namespace, key, value, version, expire);
	}
	
	public ResultCode saddAsync(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		return setImpl.saddAsync(namespace, key, value, version, expire, null); 
	}

	public ResultCode saddAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, TairCallback cb) {
		return setImpl.saddAsync(namespace, key, value, version, expire, cb); 
	}
	
	public ResultCode msadd(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
			int expire) {
		return setImpl.msadd(namespace, kvs, expire);
	}
	
	public ResultCode msaddAsync(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
					int expire, TairCallback cb) {
		return setImpl.msaddAsync(namespace, kvs, expire, cb);
	}
	
	public Result<DataEntryLong> scard(short namespace, Serializable key) {
		return setImpl.scard(namespace, key);
	}

	public Result<DataEntrySet> smembers(short namespace, Serializable key) {
		return setImpl.smembers(namespace, key);
	}

	public Result<Set<DataEntrySet>> msmembers(
			short namespace, List<? extends Serializable> keys) {
		return setImpl.msmembers(namespace, keys);
	}
	
	public Result<DataEntrySimple> spop(short namespace, Serializable key,
			 short version,int expire) {
		return setImpl.spop(namespace, key, version, expire);
	}

	public ResultCode spopAsync(short namespace, Serializable key,
			short version, int expire) {
		return setImpl.spopAsync(namespace, key, version, expire, null);
	}

	public ResultCode spopAsync(short namespace, Serializable key,
			short version, int expire, TairCallback cb) {
		return setImpl.spopAsync(namespace, key, version, expire, cb);
	}
	
	public ResultCode srem(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		return setImpl.srem(namespace, key, value, version, expire);
	}

	public ResultCode sremAsync(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		return setImpl.sremAsync(namespace, key, value, version, expire, null);
	}

	public ResultCode sremAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, TairCallback cb) {
		return setImpl.sremAsync(namespace, key, value, version, expire, cb);
	}

	public ResultCode msrem(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
					int expire) {
		return setImpl.msrem(namespace, kvs, expire);
	}

	public ResultCode msremAsync(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
					int expire, TairCallback cb) {
		return setImpl.msremAsync(namespace, kvs, expire, cb);
	}

//*********************************COMMON API*********************************************//
	public ResultCode lazyRemoveArea(int namespace) {
		return super.lazyRemoveArea(namespace);
	}
	
	public ResultCode exists(short namespace, Serializable key) {
		if (isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		RequestExistsPacket request = helpNewRequest(RequestExistsPacket.class,	
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		ReturnPacket response = sendRequest(namespace, key, request, ReturnPacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		}
		checkConfigVersion(response);
	
		return ResultCode.valueOf(response.getCode());
	}
	
	public Result<DataEntryLong> ttl(short namespace, Serializable key) {
		if (isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestTTLPacket request = helpNewRequest(RequestTTLPacket.class,	
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseTTLPacket response = sendRequest(namespace, key, request, ResponseTTLPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		checkConfigVersion(response);
	
		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, response.getValue())
		);
	}
	
	public Result<DataEntryType> type(short namespace, Serializable key) {
		
		if (isValidNamespace(namespace) == false) {
			return new Result<DataEntryType>(ResultCode.NSERROR);
		}
		
		RequestTypePacket request = helpNewRequest(RequestTypePacket.class,	
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryType>(resCode);
		
		ResponseTypePacket response = sendRequest(namespace, key, request, ResponseTypePacket.class);
		if (response == null) {
			return new Result<DataEntryType>(ResultCode.CONNERROR);
		}
		checkConfigVersion(response);
		
		return new Result<DataEntryType>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryType(key, response.getValue())
		);
	}

	public ResultCode expire(short namespace, Serializable key, int expiretime) {
		
		if (isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		if (expiretime < 0) {
			return ResultCode.SUCCESS;
		}
		
		expiretime = TairUtil.getDuration(expiretime);
		
		RequestExpirePacket request = helpNewRequest(RequestExpirePacket.class,
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setExpire(expiretime);
		
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) {
			return resCode;
		}
		
		ResponseExpirePacket response = sendRequest(namespace, key, request, ResponseExpirePacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		}
		checkConfigVersion(response);
		
		return ResultCode.valueOf(response.getCode());
	}
	

	public ResultCode addFilter(short namespace, Serializable keyPat,
			Serializable fieldPat, Serializable valuePat) {
    	if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
    		return ResultCode.NSERROR;
    	}
    	
		RequestAddFilterPacket request = helpNewRequest(RequestAddFilterPacket.class,	
				namespace);
		request.setKeyPat(keyPat);
		request.setFieldPat(fieldPat);
		request.setValuePat(valuePat);
    	
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
    	
    	boolean flag = false;
    	ResultCode resultCode = ResultCode.CONNERROR;
    	Set<Long> aliveNodes = configServer.getAliveNodes();
    	for(Long aliveNode : aliveNodes) {
    		BasePacket returnPacket = sendRequest(namespace, aliveNode, request, new TairSendRequestStatus());
            if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
                ReturnPacket r = (ReturnPacket) returnPacket;
                if (flag == false) {
                	resultCode = ResultCode.valueOf(r.getCode());
                }
                if (resultCode != ResultCode.SUCCESS) {
                	flag = true;
                }
                checkConfigVersion(returnPacket);
                if (flag == true) {
                	return resultCode;
                }
            }
    	}
    	return resultCode;
	}

	public ResultCode removeFilter(short namespace, Serializable keyPat,
			Serializable fieldPat, Serializable valuePat) {
    	if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
    		return ResultCode.NSERROR;
    	}
    	
		RequestRemoveFilterPacket request = helpNewRequest(RequestRemoveFilterPacket.class,	
				namespace);
		request.setKeyPat(keyPat);
		request.setFieldPat(fieldPat);
		request.setValuePat(valuePat);
    	
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
    	
    	boolean flag = false;
    	ResultCode resultCode = ResultCode.CONNERROR;
    	Set<Long> aliveNodes = configServer.getAliveNodes();
    	for(Long aliveNode : aliveNodes) {
    		BasePacket returnPacket = sendRequest(namespace, aliveNode, request, new TairSendRequestStatus());
            if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
                ReturnPacket r = (ReturnPacket) returnPacket;
                if (flag == false) {
                	resultCode = ResultCode.valueOf(r.getCode());
                }
                if (resultCode != ResultCode.SUCCESS) {
                	flag = true;
                }
                checkConfigVersion(returnPacket);
                if (flag == true) {
                	return resultCode;
                }
            }
    	}
    	return resultCode;
	}
	
	public ResultCode dumpArea(short namespace) {
    	if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
    		return ResultCode.NSERROR;
    	}
    	
    	RequestDumpAreaPacket request = helpNewRequest(RequestDumpAreaPacket.class,	
				namespace);
    	
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
    	
    	boolean flag = false;
    	ResultCode resultCode = ResultCode.CONNERROR;
    	Set<Long> aliveNodes = configServer.getAliveNodes();
    	for(Long aliveNode : aliveNodes) {
    		BasePacket returnPacket = sendRequest(namespace, aliveNode, request, new TairSendRequestStatus());
            if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
                ReturnPacket r = (ReturnPacket) returnPacket;
                if (flag == false) {
                	resultCode = ResultCode.valueOf(r.getCode());
                }
                if (resultCode != ResultCode.SUCCESS) {
                	flag = true;
                }
                checkConfigVersion(returnPacket);
                if (flag == true) {
                	return resultCode;
                }
            }
    	}
    	return resultCode;
	}
	
	public ResultCode loadArea(short namespace) {
    	if (namespace < 0 || namespace > TairConstant.NAMESPACE_MAX) {
    		return ResultCode.NSERROR;
    	}
    	
    	RequestLoadAreaPacket request = helpNewRequest(RequestLoadAreaPacket.class,	
				namespace);
    	
		ResultCode resCode = tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
    	
    	boolean flag = false;
    	ResultCode resultCode = ResultCode.CONNERROR;
    	Set<Long> aliveNodes = configServer.getAliveNodes();
    	for(Long aliveNode : aliveNodes) {
    		BasePacket returnPacket = sendRequest(namespace, aliveNode, request, new TairSendRequestStatus());
            if ((returnPacket != null) && returnPacket instanceof ReturnPacket) {
                ReturnPacket r = (ReturnPacket) returnPacket;
                if (flag == false) {
                	resultCode = ResultCode.valueOf(r.getCode());
                }
                if (resultCode != ResultCode.SUCCESS) {
                	flag = true;
                }
                checkConfigVersion(returnPacket);
                if (flag == true) {
                	return resultCode;
                }
            }
    	}
    	return resultCode;
	}
	

	public ResultCode setNamespaceAttr(short namespace, NSAttr attr,
			String value) {
		// TODO Auto-generated method stub
		return null;
	}

	public Result<DataEntrySimple> getNamespaceAttr(short namespace, NSAttr attr) {
		// TODO Auto-generated method stub
		return null;
	}
	
//--------------------------------------NOT SUPPORT-------------------------------------------//
	@Deprecated
	public ResultCode append(int namespace, byte[] key, byte[] value) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}
	
	@Deprecated
	public ResultCode invalid(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	@Deprecated
	public ResultCode minvalid(int namespace, List<? extends Object> keys) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}
	
	@Deprecated
	public ResultCode lock(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	@Deprecated
	public ResultCode unlock(int namespace, Serializable key) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}

	@Deprecated
	public Result<List<Object>> mlock(int namespace, List<? extends Object> keys) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	@Deprecated
	public Result<List<Object>> mlock(int namespace, List<? extends Object> keys,
			Map<Object, ResultCode> failKeysMap) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	@Deprecated
	public Result<List<Object>> munlock(int namespace, List<? extends Object> keys) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}

	@Deprecated
	public Result<List<Object>> munlock(int namespace, List<? extends Object> keys,
			Map<Object, ResultCode> failKeysMap) {
		return new Result<List<Object>>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}
	
	@Deprecated
	public ResultCode addItems(int namespace, Serializable key,
			List<? extends Object> items, int maxCount, int version,
			int expireTime) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}
	
	@Deprecated
	public Result<Integer> getItemCount(int namespace, Serializable key) {
		return new Result<Integer>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}
	
	@Deprecated
	public Result<DataEntry> getItems(int namespace, Serializable key,
			int offset, int count) {
		return new Result<DataEntry>(ResultCode.TAIR_IS_NOT_SUPPORT);
	}
	
	@Deprecated
	public ResultCode removeItems(int namespace, Serializable key, int offset,
			int count) {
		return ResultCode.TAIR_IS_NOT_SUPPORT;
	}
}
