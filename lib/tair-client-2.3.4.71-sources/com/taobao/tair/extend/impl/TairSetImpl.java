package com.taobao.tair.extend.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairBaseCallback;
import com.taobao.tair.TairCallback;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.extend.DataEntryLong;
import com.taobao.tair.extend.DataEntrySet;
import com.taobao.tair.extend.DataEntrySimple;
import com.taobao.tair.extend.TairManagerSet;
import com.taobao.tair.extend.packet.set.request.RequestSAddMultiPacket;
import com.taobao.tair.extend.packet.set.request.RequestSAddPacket;
import com.taobao.tair.extend.packet.set.request.RequestSCardPacket;
import com.taobao.tair.extend.packet.set.request.RequestSMembersMultiPacket;
import com.taobao.tair.extend.packet.set.request.RequestSMembersPacket;
import com.taobao.tair.extend.packet.set.request.RequestSPopPacket;
import com.taobao.tair.extend.packet.set.request.RequestSRemMultiPacket;
import com.taobao.tair.extend.packet.set.request.RequestSRemPacket;
import com.taobao.tair.extend.packet.set.response.ResponseSAddPacket;
import com.taobao.tair.extend.packet.set.response.ResponseSCardPacket;
import com.taobao.tair.extend.packet.set.response.ResponseSMembersMultiPacket;
import com.taobao.tair.extend.packet.set.response.ResponseSMembersPacket;
import com.taobao.tair.extend.packet.set.response.ResponseSPopPacket;
import com.taobao.tair.extend.packet.set.response.ResponseSRemPacket;
import com.taobao.tair.packet.BasePacket;

public class TairSetImpl implements TairManagerSet {
	final static long MULTI_REQUEST_WAIT_DEFAULT_TIMEOUT = 2 * 1000;
	
	final static Logger logger = Logger.getLogger(TairSetImpl.class);
	
	private TairManagerSession session;
	
	public TairSetImpl(TairManagerSession s) {
		this.session = s;
	}
	
	public ResultCode sadd(short namespace, Serializable key, Serializable value,
			short version, int expire) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestSAddPacket request = session.helpNewRequest(RequestSAddPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		ResponseSAddPacket response = session.sendRequest(namespace, key, request, ResponseSAddPacket.class);
		if (response == null) {
			return  ResultCode.CONNERROR;
		} 
		session.checkConfigVersion(response);

		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode saddAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, final TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestSAddPacket request = session.helpNewRequest(RequestSAddPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		return session.sendAsyncRequest(namespace, key, request, false,
				new TairBaseCallback(cb), SERVER_TYPE.DATA_SERVER);
	}

	public Result<DataEntryLong> scard(short namespace, Serializable key) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestSCardPacket request = session.helpNewRequest(RequestSCardPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) {
			return new Result<DataEntryLong>(resCode);
		}
		
		ResponseSCardPacket response = session.sendRequest(namespace, key, request, ResponseSCardPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, response.getValue())
		);
	}

	public Result<DataEntrySet> smembers(short namespace, Serializable key) {
		
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntrySet>(ResultCode.NSERROR);
		}
		
		RequestSMembersPacket request = session.helpNewRequest(RequestSMembersPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) {
			return new Result<DataEntrySet>(resCode);
		}

		ResponseSMembersPacket response = session.sendRequest(namespace, key, request, ResponseSMembersPacket.class);
		if (response == null) {
			return  new Result<DataEntrySet> (ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
		
		resCode = ResultCode.valueOf(response.getResultCode());
		Set<Object> values = response.getValues();
		DataEntrySet entry = null;
		if (resCode == ResultCode.SUCCESS && values != null && values.size() > 0) {
			entry = new DataEntrySet(key, values, response.getVersion());	
		}
		
		return new Result<DataEntrySet>(resCode, entry);
	}

	public Result<DataEntrySimple> spop(short namespace, Serializable key,
			 short version,int expire) {
		
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntrySimple>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestSPopPacket request = session.helpNewRequest(RequestSPopPacket.class, 
				namespace, key, version, expire);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) {
			return new Result<DataEntrySimple>(resCode);
		}


		ResponseSPopPacket response = session.sendRequest(namespace, key, request, ResponseSPopPacket.class);
		if (response == null) {
			return  new Result<DataEntrySimple> (ResultCode.CONNERROR);
		} 
		
		return new Result<DataEntrySimple>(
				 ResultCode.valueOf(response.getResultCode()), 
				 new DataEntrySimple(key, response.getValue(),
							response.getVersion())
		);
	}

	public ResultCode spopAsync(short namespace, Serializable key,
			short version, int expire, final TairCallback cb) {
		
		if (!session.isValidNamespace(namespace)) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestSPopPacket request = session.helpNewRequest(RequestSPopPacket.class, 
				namespace, key, version, expire);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) {
			return resCode;
		}

		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public ResultCode srem(short namespace, Serializable key,
			Serializable value, short version, int expire) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestSRemPacket request = session.helpNewRequest(RequestSRemPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		ResponseSRemPacket response = session.sendRequest(namespace, key, request, ResponseSRemPacket.class);
		if (response == null) {
			return  ResultCode.CONNERROR;
		} 
		session.checkConfigVersion(response);

		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode sremAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, final TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestSRemPacket request = session.helpNewRequest(RequestSRemPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}

	public ResultCode msaddAsync(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
			int expire, TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		ResultCode code = null;
		
		Set<? extends Serializable> keys = kvs.keySet();
		Map<Long, Set<Serializable>> buckets = null;
		try {
			buckets = session.classifyKeys(keys);
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage());
			return ResultCode.SERIALIZEERROR;
		}
		
		List<Map<? extends Serializable, Set<? extends Serializable>>> list =
				new ArrayList<Map<? extends Serializable, Set<? extends Serializable>>>();
		Set<Entry<Long,Set<Serializable>>> collections = buckets.entrySet();
		for(Entry<Long, Set<Serializable>> entry : collections) {
			Long serverIp = entry.getKey();
			Set<Serializable> bucket = entry.getValue();
			Map<Serializable, Set<? extends Serializable>> key_values =
					new HashMap<Serializable, Set<? extends Serializable>>();
			list.add(key_values);
			for(Serializable key : bucket) {
				Set<? extends Serializable> values = kvs.get(key);
				key_values.put(key, values);
			}
			
			RequestSAddMultiPacket request = session.helpNewRequest(RequestSAddMultiPacket.class, 
					namespace, expire);
			request.setKeysValues(key_values);
			
			code = session.tryEncode(request);
			if (!code.isSuccess()) {
				return code;
			}
			
			code = session.sendAsyncRequest(namespace, serverIp, request, false, new TairBaseCallback(cb),
					SERVER_TYPE.DATA_SERVER);

			if (code != ResultCode.SUCCESS && code != ResultCode.TARGET_ALREADY_EXIST) {
				logger.warn("sadd multi packet: " + code.toString());
				return code;
			}
		}
		
		return code;
	}

	public ResultCode msadd(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
			int expire) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		ResultCode code = null;
		
		Set<? extends Serializable> keys = kvs.keySet();
		Map<Long, Set<Serializable>> buckets = null;
		try {
			buckets = session.classifyKeys(keys);
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage());
			return ResultCode.SERIALIZEERROR;
		}
		
		final CountDownLatch countDownLatch = new CountDownLatch(buckets.size());
		final AtomicBoolean failed = new AtomicBoolean(false);
		final AtomicReference<ResultCode> atomicResultCode = new AtomicReference<ResultCode>();
		atomicResultCode.set(ResultCode.SUCCESS);
		
		List<Map<? extends Serializable, Set<? extends Serializable>>> list =
				new ArrayList<Map<? extends Serializable, Set<? extends Serializable>>>();
		Set<Entry<Long,Set<Serializable>>> collections = buckets.entrySet();
		for(Entry<Long, Set<Serializable>> entry : collections) {
			Long serverIp = entry.getKey();
			Set<Serializable> bucket = entry.getValue();
			Map<Serializable, Set<? extends Serializable>> key_values =
					new HashMap<Serializable, Set<? extends Serializable>>();
			list.add(key_values);
			for(Serializable key : bucket) {
				Set<? extends Serializable> values = kvs.get(key);
				key_values.put(key, values);
			}
			
			RequestSAddMultiPacket request = session.helpNewRequest(RequestSAddMultiPacket.class, 
					namespace, expire);
			request.setKeysValues(key_values);
			
			code = session.tryEncode(request);
			if (!code.isSuccess()) {
				return code;
			}
			
			if (failed.compareAndSet(true, false)) {
				return ResultCode.ASYNCERR;
			}
			
			code = session.sendAsyncRequest(namespace, serverIp, request, false, new TairCallback() {

				public void callback(Exception e) {
					logger.warn(e.getMessage());
					failed.set(true);
					countDownLatch.countDown();
				}
				
				public void callback(BasePacket packet) {
					if (!(packet instanceof ResponseSAddPacket)) {
						logger.warn("packet " + packet.getPcode() + "is not ResponseSAddPacket");
						failed.set(true);
					} else {
						ResponseSAddPacket response = (ResponseSAddPacket)packet;
						if (response.getCode() != ResultCode.SUCCESS.getCode() &&
								ResultCode.valueOf(response.getCode()) != ResultCode.TARGET_ALREADY_EXIST) {
							atomicResultCode.set(ResultCode.valueOf(response.getCode()));
							failed.set(true);
						}
					}
					countDownLatch.countDown();
				}
			}, SERVER_TYPE.DATA_SERVER);

			if (code != ResultCode.SUCCESS && code != ResultCode.TARGET_ALREADY_EXIST) {
				logger.warn("sadd multi packet: " + code.toString());
				return code;
			}
		}

		try {
			boolean ok = countDownLatch.await(MULTI_REQUEST_WAIT_DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
			code = atomicResultCode.get();
			if (ok == false) {
				return ResultCode.TIMEOUT;
			} else if (failed.compareAndSet(true, false) && code == ResultCode.SUCCESS) {
				return ResultCode.ASYNCERR;	
			}
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
			return ResultCode.ASYNCERR;
		}
		
		return code;
	}

	public Result<Set<DataEntrySet>> msmembers(
			short namespace, List<? extends Serializable> keys) {
		if (session.isValidNamespace(namespace) == false) {
			return new Result<Set<DataEntrySet>>(ResultCode.NSERROR);
		}
		
		Map<Long, Set<Serializable>> buckets = null;
		try {
			buckets = session.classifyKeys(keys);
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage());
			return new Result<Set<DataEntrySet>>(ResultCode.SERIALIZEERROR);
		}
		
		final Map<Integer, Set<Serializable>> chidkeysMaper = new ConcurrentHashMap<Integer, Set<Serializable>>();
		final Set<DataEntrySet> result = new ConcurrentSkipListSet<DataEntrySet>(new Comparator<DataEntrySet>() {
			public int compare(DataEntrySet o1, DataEntrySet o2) {
				return o1.hashCode() - o2.hashCode();
			}
		});
		ResultCode code = null;
		final CountDownLatch countDownLatch = new CountDownLatch(buckets.size());
		final AtomicReference<ResultCode> atomicResultCode = new AtomicReference<ResultCode>();
		atomicResultCode.set(ResultCode.SUCCESS);
		final AtomicBoolean failed = new AtomicBoolean(false);
		
		Set<Entry<Long, Set<Serializable>>> bucketSets = buckets.entrySet();
		for(Entry<Long, Set<Serializable>> key : bucketSets) {
			Long serverIp = key.getKey();
			Set<Serializable> key_part = key.getValue();
			final RequestSMembersMultiPacket request = session.helpNewRequest(RequestSMembersMultiPacket.class,
					namespace, TairConstant.NOT_CARE_EXPIRE);
			request.setKeys(key_part);
			code = session.tryEncode(request);
			if (!code.isSuccess()) {
				return new Result<Set<DataEntrySet>>(code);
			}
			chidkeysMaper.put(request.getChid(), key_part);
			
			code = session.sendAsyncRequest(namespace, serverIp, request, false, new TairCallback() {
				
				public void callback(Exception e) {
					logger.warn(e.getMessage());
					failed.set(true);
					countDownLatch.countDown();
				}
				
				public void callback(BasePacket packet) {
					if (!(packet instanceof ResponseSMembersMultiPacket)) {
						logger.warn("packet " + packet.getPcode() + "is not ResponseSRemMultiPacket");
						failed.set(true);
					} else {
						ResponseSMembersMultiPacket response = (ResponseSMembersMultiPacket)packet;
						if (response.getResultCode() != ResultCode.SUCCESS.getCode() &&
								ResultCode.valueOf(response.getResultCode()) != ResultCode.DATANOTEXSITS) {
							atomicResultCode.set(ResultCode.valueOf(response.getResultCode()));
						} else {
							int chid = request.getChid();
							Set<Serializable> key_part = chidkeysMaper.get(chid);
							List<Entry<Short, Set<Object>>> retValues = response.getValueSetList();
//							if (key_part == null) {
//								logger.error("key_part == null");
//								failed.setSync(true);
//							} 
							int key_part_len = (key_part == null ? 0 :key_part.size());
							if (key_part_len <= 0 || key_part_len != retValues.size() ) {
								logger.error("key_part_len " + key_part_len +
										" != retValues.size()" + retValues.size());
								failed.set(true);
							} else {
								int index = 0;
								for(Serializable key : key_part) {
									Entry<Short, Set<Object>> entry = retValues.get(index);
									DataEntrySet des = new DataEntrySet(key, entry.getValue(), entry.getKey());
									result.add(des);
									index++;
								}
							}
						}
					}
					
					countDownLatch.countDown();
				}
			}, SERVER_TYPE.DATA_SERVER);
			
			if (code != ResultCode.SUCCESS && code != ResultCode.DATANOTEXSITS) {
				logger.warn("smembers multi packet: " + code.toString());
				return new Result<Set<DataEntrySet>>(code);
			}
		}

		try {
			boolean ok = countDownLatch.await(MULTI_REQUEST_WAIT_DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
			code = atomicResultCode.get();
			if (ok == false) {
				return new Result<Set<DataEntrySet>>(ResultCode.TIMEOUT);
			} else if (failed.compareAndSet(true, false)) {
				if (code == ResultCode.SUCCESS) {
					return new Result<Set<DataEntrySet>>(ResultCode.ASYNCERR);
				} else {
					return new Result<Set<DataEntrySet>>(code);
				}
			}
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
			return new Result<Set<DataEntrySet>>(ResultCode.ASYNCERR);
		}
		
		Set<DataEntrySet> result_set = new HashSet<DataEntrySet>(result);
		return new Result<Set<DataEntrySet>>(code, result_set);
	}

	public ResultCode msremAsync(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
			int expire, final TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		ResultCode code = null;
		
		Set<? extends Serializable> keys = kvs.keySet();
		Map<Long, Set<Serializable>> buckets = null;
		try {
			buckets = session.classifyKeys(keys);
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage());
			return ResultCode.SERIALIZEERROR;
		}
		
		List<Map<? extends Serializable, Set<? extends Serializable>>> list =
				new ArrayList<Map<? extends Serializable, Set<? extends Serializable>>>();
		Set<Entry<Long,Set<Serializable>>> collections = buckets.entrySet();
		for(Entry<Long, Set<Serializable>> entry : collections) {
			Long serverIp = entry.getKey();
			Set<Serializable> bucket = entry.getValue();
			Map<Serializable, Set<? extends Serializable>> key_values =
					new HashMap<Serializable, Set<? extends Serializable>>();
			list.add(key_values);
			for(Serializable key : bucket) {
				Set<? extends Serializable> values = kvs.get(key);
				key_values.put(key, values);
			}
			
			RequestSRemMultiPacket request = session.helpNewRequest(RequestSRemMultiPacket.class, 
					namespace, expire);
			request.setKeysValues(key_values);
			
			code = session.tryEncode(request);
			if (!code.isSuccess()) {
				return code;
			}
			
			code = session.sendAsyncRequest(namespace, serverIp, request, false,
					new TairBaseCallback(cb), SERVER_TYPE.DATA_SERVER);
			
			if (code != ResultCode.SUCCESS && code != ResultCode.DATANOTEXSITS) {
				logger.warn("sadd multi packet: " + code.toString());
				return code;
			}
		}
		
		return code;
	}
	
	public ResultCode msrem(short namespace,
			Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
					int expire) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		ResultCode code = null;
		
		Set<? extends Serializable> keys = kvs.keySet();
		Map<Long, Set<Serializable>> buckets = null;
		try {
			buckets = session.classifyKeys(keys);
		} catch (IllegalArgumentException e) {
			logger.debug(e.getMessage());
			return ResultCode.SERIALIZEERROR;
		}
		
		final CountDownLatch countDownLatch = new CountDownLatch(buckets.size());
		final AtomicBoolean failed = new AtomicBoolean(false);
		final AtomicReference<ResultCode> atomicResultCode = new AtomicReference<ResultCode>();
		atomicResultCode.set(ResultCode.SUCCESS);
		
		List<Map<? extends Serializable, Set<? extends Serializable>>> list =
				new ArrayList<Map<? extends Serializable, Set<? extends Serializable>>>();
		Set<Entry<Long,Set<Serializable>>> collections = buckets.entrySet();
		for(Entry<Long, Set<Serializable>> entry : collections) {
			Long serverIp = entry.getKey();
			Set<Serializable> bucket = entry.getValue();
			Map<Serializable, Set<? extends Serializable>> key_values =
					new HashMap<Serializable, Set<? extends Serializable>>();
			list.add(key_values);
			for(Serializable key : bucket) {
				Set<? extends Serializable> values = kvs.get(key);
				key_values.put(key, values);
			}
			
			RequestSRemMultiPacket request = session.helpNewRequest(RequestSRemMultiPacket.class, 
					namespace, expire);
			request.setKeysValues(key_values);
			
			code = session.tryEncode(request);
			if (!code.isSuccess()) {
				return code;
			}
			
			if (failed.compareAndSet(true, false)) {
				return ResultCode.ASYNCERR;
			}
			
			code = session.sendAsyncRequest(namespace, serverIp, request, false, new TairCallback() {

				public void callback(Exception e) {
					logger.warn(e.getMessage());
					failed.set(true);
					countDownLatch.countDown();
				}
				
				public void callback(BasePacket packet) {
					if (!(packet instanceof ResponseSRemPacket)) {
						logger.warn("packet " + packet.getPcode() + "is not ResponseSAddPacket");
						failed.set(true);
					} else {
						ResponseSRemPacket response = (ResponseSRemPacket)packet;
						if (response.getCode() != ResultCode.SUCCESS.getCode() &&
								ResultCode.valueOf(response.getCode()) != ResultCode.DATANOTEXSITS) {
							atomicResultCode.set(ResultCode.valueOf(response.getCode()));
							failed.set(true);
						}
					}
					countDownLatch.countDown();
				}
			}, SERVER_TYPE.DATA_SERVER);
			
			if (code != ResultCode.SUCCESS && code != ResultCode.DATANOTEXSITS) {
				logger.warn("sadd multi packet: " + code.toString());
				return code;
			}
		}

		try {
			boolean ok = countDownLatch.await(MULTI_REQUEST_WAIT_DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
			code = atomicResultCode.get();
			if (ok == false) {
				return ResultCode.TIMEOUT;
			} else if (failed.compareAndSet(true, false) && code == ResultCode.SUCCESS) {
				return ResultCode.ASYNCERR;	
			}
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
			return ResultCode.ASYNCERR;
		}
		
		return code;
	}
}
