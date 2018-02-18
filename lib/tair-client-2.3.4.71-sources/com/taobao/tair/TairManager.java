/**
 *
 */
package com.taobao.tair;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tair.comm.DataEntryLocalCache;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.CounterPack;
import com.taobao.tair.etc.KeyCountPack;
import com.taobao.tair.etc.KeyValuePack;
import com.taobao.tair.impl.ConfigServer;

/**
 * Tair Client Interface
 *
 *
 */
public interface TairManager {
	/**
	 * open local cache for the namespace.
	 *
	 * @param namespace
	 *            the namespace for opening the local cache.
	 * @param capacity
	 *            the capacity of the local cache
	 * @param expire
	 *            the expire time of the local cache for holding the item.
	 * @param cl
	 *            user definded class loader
	 */
	public void setupLocalCache(int namespace, int cap, long exp, ClassLoader cl);

	public void setupLocalCache(int namespace, int cap, long exp);

	public void setupLocalCache(int namespace);

	public void enhanceLocalCache(int namespace);

	public Map<Integer, DataEntryLocalCache> getAllLocalCache();

	public void destroyLocalCache(int namespace);

	public void destroyAllLocalCache();

	public void setSupportBackupMode(boolean supportBackupMode);

	public String getCharset();

	public void setCharset(String charset);

	public int getCompressionThreshold();

	public void setCompressionThreshold(int compressionThreshold);

	public int getCompressionType();

	public void setCompressionType(int compressionType);

	public List<String> getConfigServerList();

	public void setConfigServerList(List<String> configServerList);

	// the format of dataServer is like "hostname:port", eg "127.0.0.1:1234"
	public void setDataServer(String dataServer);

	public String getGroupName();

	public void setGroupName(String groupName);

	public int getMaxWaitThread();

	public int getTimeout();

	public void setTimeout(int timeout);

	/**
	 * 注意这个函数，应该是设置主机的timeout吧
	 * @param timeout
	 */
	public void setAdminTimeout(int timeout);

	public String toString();

	public ConfigServer getConfigServer();

	public void setHeader(boolean flag);

	public void setMaxWaitThread(int maxThreadCount);

	public ClassLoader getCustomClassLoader();

	public void setCustomClassLoader(ClassLoader customClassLoader);

	/**
	 * 猜不出这个函数是干什么用的
	 * @param keys
	 * @return
	 * @throws IllegalArgumentException
	 */
	public Map<Long, Set<Serializable>> classifyKeys(
			Collection<? extends Serializable> keys)
			throws IllegalArgumentException;

	public int getNamespaceOffset();

	public void setNamespaceOffset(int namespaceOffset);

	public void init();

	/**
	 * 看不出这个函数是干啥用的
	 * 查了网上，扯到 单向数据流应用库 啥的
	 * @param ratio
	 */
	public void setRefluxRatio(int ratio);

	public void resetHappendDownServer();

	public Map<String, String> notifyStat();

	/**
	 * Read key from Tair
	 *
	 * @param namespace
	 *            the namespace for read operation
	 * @param key
	 *            just the key.
	 * @return SUCCESS, every thing was ok, you got your data. TIMEOUT, RPC
	 *         timeout. CONNERROR, some exceptions were occurred. DATANOTEXIST,
	 *         the key was not exist
	 */
	Result<DataEntry> get(int namespace, Serializable key);

	/**
	 * Batch read keys from Tair
	 *
	 * @param namespace
	 *            the namespace for read operation
	 * @param keys
	 *            the set keys
	 * @return SUCCESS, every thing was ok, you got your data. PARTSUCCESS, only
	 *         the part of request(s) were ok. TIMEOUT, RPC timeout. CONNERROR,
	 *         some exceptions were occurred. DATANOTEXIST, the key was not
	 *         exist
	 */
	Result<List<DataEntry>> mget(int namespace, List<? extends Object> keys);

	/**
	 * Read key from Tair with expire time, only support alipay clusters yet
	 *
	 * @param namespace
	 *            the namespace for read operation
	 * @param key
	 *            just the key.
	 * @param expireTime
	 *            unit is in milliseconds
	 * @return SUCCESS, every thing was ok, you got your data. TIMEOUT, RPC
	 *         timeout. CONNERROR, some exceptions were occurred. DATANOTEXIST,
	 *         the key was not exist
	 */
	Result<DataEntry> get(int namespace, Serializable key, int expireTime);

	/**
	 * Batch put
	 * @param namespace
	 *        the namespace for read operation
	 * @param kvRecords
	 * 		  key-value set
	 * @param compress
	 * 		  compress flag
	 * @return
	 *        SUCCESS, successful
	 *        PARTSUCCESS, exist some key-value(s) failed.
	 *        TIMEOUT or ERROR, exceptions had occurred.
	 */
	ResultCode mput(int namespace, List<KeyValuePack> kvRecords, boolean compress);
	/**
	 * Write data to Tair, the key was never expired, and do not check the
	 * version.
	 *
	 * @param namespace
	 *            the namespace for write key
	 * @param key
	 *            just the key
	 * @param value
	 *            the value you want to write
	 * @return SUCCESS, every thing was ok. TIMEOUT, RCP timeout, CONNERROR,
	 *         some exceptions were occurred.
	 */
	ResultCode put(int namespace, Serializable key, Serializable value);

	/**
	 * @param namespace
	 *            鏁版嵁鎵�湪鐨刵amespace
	 * @param key
	 * @param value
	 * @return
	 */
	ResultCode putAsync(int namespace, Serializable key, Serializable value,
			int version, int expireTime, boolean fillCache, TairCallback cb);

	/**
	 * Write data to Tair, the key was never expired.
	 *
	 * @param namespace
	 *            the namespace for write key
	 * @param key
	 *            just the key
	 * @param value
	 *            the value you want to write
	 * @param version
	 *            default value was 0, that means tair will always write the key
	 *            without checking the version. otherwise put successfully only
	 *            if the input version was equal to the version at the server
	 *            side.
	 * @return SUCCESS, every thing was ok. TIMEOUT, RCP timeout, CONNERROR,
	 *         some exceptions were occurred.
	 */
	ResultCode put(int namespace, Serializable key, Serializable value,
			int version);

	/**
	 * Write data to Tair
	 *
	 * @param namespace
	 *            the namespace for write key
	 * @param key
	 *            just the key
	 * @param value
	 *            the value you want to write
	 * @param version
	 *            default value was 0, that means tair will always write the key
	 *            without checking the version. otherwise put successfully only
	 *            if the input version was equal to the version at the server
	 *            side.
	 * @param expireTime
	 *            unit is in milliseconds, default value was 0, that means the key-value was never
	 *            expired. otherwise, the value should be absolute time or
	 *            relative time. after the expire time, the key was removed by
	 *            server.
	 * @return SUCCESS, every thing was OK. TIMEOUT, RCP timeout, CONNERROR,
	 *         some exceptions were occurred.
	 */
	ResultCode put(int namespace, Serializable key, Serializable value,
			int version, int expireTime);

	ResultCode put(int namespace, Serializable key, Serializable value,
			int version, int expireTime, boolean fillCache);

	/**
	 * Delete key from Tair
	 *
	 * @param namespace
	 *            the namespace for delete operation
	 * @param key
	 *            just the key.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. DATANOTEXIST, the key was not exist
	 */
	ResultCode delete(int namespace, Serializable key);

	/**
	 * 这个跟delete有什么区别？
	 * delete through 'invalid server', the key/values with the groupname in
	 * several corresponding clusters would all be deleted.
	 *
	 * @param namespace
	 *            area/namespace the key belongs to.
	 * @param key
	 *            key to invalid.
	 * @return
	 */
	ResultCode invalid(int namespace, Serializable key);

	/**
	 * same as invalid(int, Serializable), but asynchronously
	 *
	 * @param namespace
	 *            area/namespace the key belongs to.
	 * @param key
	 *            key to invalid.
	 * @return
	 */
	ResultCode invalid(int namespace, Serializable key, CallMode callMode);

	/**
	 * hide the key/value, after which 'get' would return 'item is hidden'
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param key
	 *            key to hide.
	 * @return
	 */
	ResultCode hide(int namespace, Serializable key);

	/**
	 * same as hide(int, Serializable), but through 'invalid server'
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param key
	 *            key to hide.
	 * @return
	 */
	ResultCode hideByProxy(int namespace, Serializable key);

	/**
	 * same as hideByProxy(int, Serializable), but could be asynchronous.
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param key
	 *            key to hide.
	 * @return
	 */
	ResultCode hideByProxy(int namespace, Serializable key, CallMode callMode);

	/**
	 * Read Hidden key from Tair
	 *
	 * @param namespace
	 *            the namespace for getHidden operation
	 * @param key
	 *            just the key.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. DATANOTEXIT, the key was not exist
	 *         TIMEOUT, RPC tiemout SERIALIZEERROR, some exception was occurred
	 *         at encode phase
	 */
	Result<DataEntry> getHidden(int namespace, Serializable key);

	/**
	 * same as prefixPut(namespace, pkey, skey, 0, 0)
	 */
	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value);

	/**
	 * same as prefixPut(namespace, pkey, skey, version, 0)
	 */
	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable skey, Serializable value, int version);

	/**
	 * Write prefix-suffix key value to Tair
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key for write
	 * @param skey
	 *            the suffix key for write
	 * @param value
	 *            the value you want to write
	 * @param version
	 *            default value was 0, that means tair will always write the key
	 *            without checking the version. otherwise put successfully only
	 *            if the input version was equal to the version at the server
	 *            side.
	 * @param expireTime
	 *            unit is in milliseconds, default value was 0, that means the key-value was never
	 *            expired. otherwise, the value should be absolute time or
	 *            relative time. after the expire time, the key was removed by
	 *            server.
	 * @return SUCCESS, every thing was ok. TIMEOUT, RCP timeout, CONNERROR,
	 *         some exception was occurred SERIALIZEERROR, some exception was
	 *         occurred at encode phase
	 */
	public ResultCode prefixPut(int namespace, Serializable pkey,
			Serializable sKey, Serializable value, int version, int expireTime);

	/**
	 * Write batch prefix-suffix keys value to Tair
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key for write
	 * @param keyValuePacks
	 *            the set of suffix keys with the value for write operation, the
	 *            KeyValuePack instance contains the version, expire time
	 *            information of the suffix key. and the means of version and
	 *            expireTime was same as put/prefixPut's parameters.
	 * @return SUCCESS, every thing was ok. TIMEOUT, RCP timeout, CONNERROR,
	 *         some exception was occurred SERIALIZEERROR, some exception was
	 *         occurred at encode phase
	 */
	public Result<Map<Object, ResultCode>> prefixPuts(int namespace,
			Serializable pkey, List<KeyValuePack> keyValuePacks);

	/**
	 * Write batch prefix-suffix keys value to Tair
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key for write
	 * @param keyValuePacks
	 *            the set of suffix keys with the value for write operation, the
	 *            KeyValuePack instance contains the version, expire time
	 *            information of the suffix key. and the means of version and
	 *            expireTime was same as put/prefixPut's parameters.
	 * @param keyCountPacks
	 *            the set of suffix keys with the counter for write operation.
	 *            the keyCountPack instance also holds the version and expire.
	 * @return SUCCESS, every thing was ok. TIMEOUT, RCP timeout, CONNERROR,
	 *         some exception was occurred SERIALIZEERROR, some exception was
	 *         occurred at encode phase
	 */
	public Result<Map<Object, ResultCode>> prefixPuts(int namespace,
			Serializable pkey, List<KeyValuePack> keyValuePacks,
			List<KeyCountPack> keyCountPacks);

	/**
	 * 这函数是干啥的？
	 * 我猜可能是一个key的前一部分是prefix，后一部分是suffix
	 * @param namespace
	 *            namespace for read
	 * @param pkey
	 *            the prefix key
	 * @param skey
	 *            the suffix key
	 * @return SUCCESS, every thing is ok. ITEM_HIDDEN, the key was hidden by
	 *         prefixHidden operation DATANOTEXISTS, the key was not exist.
	 *         TIMEOUT, rpc was timeout CONNERROR, connection error
	 */
	public Result<DataEntry> prefixGet(int namespace, Serializable pkey,
			Serializable skey);

	/**
	 * @param namespace
	 *            namespace for hide operation
	 * @param pkey
	 *            the prefix key
	 * @param skeys
	 *            the set of suffix key
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout CONNERROR,
	 *         connection error
	 */
	public Result<Map<Object, Result<DataEntry>>> prefixGets(int namespace,
			Serializable pkey, List<? extends Serializable> skeyList);

	/**
	 * delete the prefix-suffix key/value, after which 'get' would return 'data
	 * not exist'
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to delete.
	 * @param skey
	 *            the suffix key to delete
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public ResultCode prefixDelete(int namespace, Serializable pkey,
			Serializable sKey);

	/**
	 * delete the batch prefix-suffix key/value, after which 'get' would return
	 * 'data not exist'
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to delete.
	 * @param skey
	 *            the suffix key to delete
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public Result<Map<Object, ResultCode>> prefixDeletes(int namespace,
			Serializable pkey, List<? extends Serializable> skeys);

	/**
	 * 可以看到这是支持计数操作的。
	 * incr the prefix-suffix key counter. if the prefix-suffix key was not
	 * exist at the server side, a new prefix-suffix counter was created, the
	 * value was equal to (defaultValue + value)
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to incr.
	 * @param skey
	 *            the suffix key to incr
	 * @param value
	 *            the step value of the incr operation
	 * @param defaultValue
	 *            the initialize value of the counter if the key was not exsit.
	 * @param expireTime
	 *            unit is in milliseconds, default value was 0, that means the key-value was never
	 *            expired. otherwise, the value should be absolute time or
	 *            relative time. after the expire time, the key was removed by
	 *            server.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public Result<Integer> prefixIncr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime);

	public Result<Integer> prefixIncr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBound, int upperBound);

	/**
	 * incr the batch prefix-suffix key counters. if the prefix-suffix key was
	 * not exist at the server side, a new prefix-suffix counter was created,
	 * the value was equal to (defaultValue + value)
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to incr.
	 * @param packList
	 *            the suffix key with value, defaultValue, expireTime to incr.
	 *            SUCCESS, every thing was ok, you got your data. CONNERROR,
	 *            some exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace,
			Serializable pkey, List<CounterPack> packList);

	public Result<Map<Object, Result<Integer>>> prefixIncrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound);

	/**
	 * decr the prefix-suffix key counter. if the prefix-suffix key was not
	 * exist at the server side, a new prefix-suffix counter was created, the
	 * value was equal to (defaultValue - value)
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to decr.
	 * @param skey
	 *            the suffix key to decr
	 * @param value
	 *            the step value of the decr operation
	 * @param defaultValue
	 *            the initialize value of the counter if the key was not exsit.
	 * @param expireTime
	 *            unit is in milliseconds, default value was 0, that means the key-value was never
	 *            expired. otherwise, the value should be absolute time or
	 *            relative time. after the expire time, the key was removed by
	 *            server.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime);

	public Result<Integer> prefixDecr(int namespace, Serializable pkey,
			Serializable skey, int value, int defaultValue, int expireTime,
			int lowBound, int upperBound);

	/**
	 * decr the batch prefix-suffix key counters. if the prefix-suffix key was
	 * not exist at the server side, a new prefix-suffix counter was created,
	 * the value was equal to (defaultValue + value)
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to decr.
	 * @param packList
	 *            the suffix key with value, defaultValue, expireTime to incr.
	 *            SUCCESS, every thing was ok, you got your data. CONNERROR,
	 *            some exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList);

	public Result<Map<Object, Result<Integer>>> prefixDecrs(int namespace,
			Serializable pkey, List<CounterPack> packList, int lowBound,
			int upperBound);

	/**
	 * same as prefixSetCount(peky, skey, count, 0, 0);
	 */
	public ResultCode prefixSetCount(int namespace, Serializable pkey,
			Serializable skey, int count);

	/**
	 * create the prefix-suffix key counter. a new prefix-suffix counter set
	 * were created, the value was equal to count.
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param pkey
	 *            the prefix key to incr.
	 * @param value
	 *            the new counter's value.
	 * @param version
	 *            same as put's version parameter.
	 * @param expireTime
	 *            same as put's expireTime parameter.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	public ResultCode prefixSetCount(int namespace, Serializable pkey,
			Serializable skey, int count, int version, int expireTime);

	public Result<Map<Object, ResultCode>> prefixSetCounts(int namespace,
			Serializable pkey, List<KeyCountPack> keyCountPacks);

	/**
	 * same as hide, but the prefix-suffix key value pair
	 */
	public ResultCode prefixHide(int namespace, Serializable pkey,
			Serializable skey);

	/**
	 * same as getHidden, but the prefix-suffix key value pair
	 */
	public Result<DataEntry> prefixGetHidden(int namespace, Serializable pkey,
			Serializable skey);

	/**
	 * Read the hidden prefix-suffix keys from Tair
	 *
	 * @param namespace
	 *            the namespace the key set belong to.
	 * @param pkey
	 *            prefix key
	 * @param skeys
	 *            the set of suffix key.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. DATANOTEXIT, the key was not exist
	 *         TIMEOUT, RPC tiemout SERIALIZEERROR, some exception was occurred
	 *         at encode phase
	 */
	public Result<Map<Object, Result<DataEntry>>> prefixGetHiddens(
			int namespace, Serializable pkey, List<? extends Serializable> skeys);

	/**
	 * hide the prefix suffix key value pairs
	 *
	 * @param namespace
	 *            the namespace the key set belong to.
	 * @param pkey
	 *            prefix key
	 * @param skeys
	 *            the set of suffix key.
	 * @return SUCCESS, every thing was ok, the operation was executed
	 *         successfully at server side. CONNERROR, some exceptions were
	 *         occurred. DATANOTEXIT, the key was not exist TIMEOUT, RPC tiemout
	 *         SERIALIZEERROR, some exception was occurred at encode phase
	 */
	public Result<Map<Object, ResultCode>> prefixHides(int namespace,
			Serializable pkey, List<? extends Serializable> skeys);

	/**
	 * 我觉得注意一下设计中有同步和异步的调用方式比较好
	 * Invalid prefix key with single key Invalserver will process this request,
	 * and remove the keys.
	 *
	 * @param namespace
	 *            namespace for invalid operation
	 * @param pkey
	 *            the prefix key
	 * @param skey
	 *            the suffix key
	 * @param callMode
	 *            not used now, just ignore it.
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout CONNERROR,
	 *         connection error
	 */
	public ResultCode prefixInvalid(int namespace, Serializable pkey,
			Serializable skey, CallMode callMode);

	/**
	 * 这个谜一样的注释
	 * hide prefix key with single key Invalserver will process this request,
	 * and hide the keys.
	 *
	 * @param namespace
	 *            namespace for hide operation
	 * @param pkey
	 *            the prefix key
	 * @param skey
	 *            the suffix key
	 * @param callMode
	 *            not used now, just ignore it.
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout CONNERROR,
	 *         connection error
	 */
	public ResultCode prefixHideByProxy(int namespace, Serializable pkey,
			Serializable skey, CallMode callMode);

	/**
	 * hide prefix key with multi suffix keys Invalserver will process this
	 * request, and hide the keys.
	 *
	 * @param namespace
	 *            namespace for hide operation
	 * @param pkey
	 *            the prefix key
	 * @param skey
	 *            the set of suffix keys
	 * @param callMode
	 *            not used now, just ignore it.
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout CONNERROR,
	 *         connection error
	 */
	public Result<Map<Object, ResultCode>> prefixHidesByProxy(int namespace,
			Serializable pkey, List<? extends Serializable> skeys,
			CallMode callMode);

	/**
	 * 感觉这个InvalServer有点迷
	 * Invalid prefix key with multi keys Invalserver will process this request,
	 * and remove the keys.
	 *
	 * @param namespace
	 *            namespace for invalid operation
	 * @param pkey
	 *            the prefix key
	 * @param skey
	 *            the suffix key
	 * @param callMode
	 *            not used now, just ignore it.
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout CONNERROR,
	 *         connection error
	 */
	public Result<Map<Object, ResultCode>> prefixInvalids(int namespace,
			Serializable pkey, List<? extends Serializable> skeys,
			CallMode callMode);

	/**
	 * Read batch prefix key with multi key values that were hidden.
	 *
	 * @param namespace
	 *            namespace for invalid operation
	 * @param pkeySkeyListMap
	 *            the set of prefix keys and suffix keys.
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout,
	 *         DATANOTEXIST, data not exist. CONNERROR, connection error
	 */
	public Result<Map<Object, Map<Object, Result<DataEntry>>>> mprefixGetHiddens(
			int namespace,
			Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap);

	/**
	 * invalid the key/value, after which 'get' would return 'data not exist'
	 *
	 * @param namespace
	 *            area, or namespace the key belongs to.
	 * @param keys
	 *            the key set to hide.
	 * @return SUCCESS, every thing was ok, you got your data. CONNERROR, some
	 *         exceptions were occurred. TIMEOUT, RPC tiemout
	 */
	ResultCode minvalid(int namespace, List<? extends Object> keys);

	/**
	 * delete multi key-value pairs
	 *
	 * @param namespace
	 *            area, or namespace the keys belong to.
	 * @param keys
	 *            the key set to be deleted
	 * @return
	 */
	ResultCode mdelete(int namespace, List<? extends Object> keys);

	/**
	 * 感觉这的key_start跟key_end可能有某种方式能遍历下来
	 * obtain the batch key-value pairs whose suffix key were in the reange
	 *
	 * @param namespace
	 *            area, or namespace the keys belong to.
	 * @param prefix
	 *            prefix key
	 * @param key_start
	 *            the begin of the suffix key range
	 * @param key_end
	 *            the end of the suffix key range
	 * @param offset
	 *            the offset of the suffix key range to scan the key range
	 * @param limit
	 *            the limit of the result count
	 * @param reverse
	 *            the order that the server scans the data
	 * @return getData() data entry list.
	 */
	Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset,
			int limit, boolean reverse);

	/**
	 * obtain the batch prefix-suffix key-value pairs whose suffix key were in
	 * the reange. return the value only if those values were exist.
	 *
	 * @param namespace
	 *            area, or namespace the keys belong to.
	 * @param prefix
	 *            prefix key
	 * @param key_start
	 *            the begin of the suffix key range
	 * @param key_end
	 *            the end of the suffix key range
	 * @param offset
	 *            the offset of the suffix key range to scan the key range
	 * @param limit
	 *            the limit of the result count
	 * @param reverse
	 *            the order that the server scans the data
	 * @return getData() data entry list.
	 */
	Result<List<DataEntry>> getRangeOnlyValue(int namespace,
			Serializable prefix, Serializable key_start, Serializable key_end,
			int offset, int limit, boolean reverse);

	/**
	 * obtain the batch prefix-suffix key-value pairs whose suffix key were in
	 * the reange. return the value only if those values were exist.
	 *
	 * @param namespace
	 *            area, or namespace the keys belong to.
	 * @param prefix
	 *            prefix key
	 * @param key_start
	 *            the begin of the suffix key range
	 * @param key_end
	 *            the end of the suffix key range
	 * @param offset
	 *            the offset of the suffix key range to scan the key range
	 * @param limit
	 *            the limit of the result count
	 * @param reverse
	 *            the order that the server scans the data
	 * @return getData() data entry list.
	 */

	Result<List<DataEntry>> getRangeOnlyKey(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset,
			int limit, boolean reverse);

	/**
	 *
	 * @param namespace
	 * @param prefix
	 * @param key_start
	 * @param key_end
	 * @param offset
	 * @param limit
	 * @return
	 */
	Result<List<DataEntry>> getRange(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset, int limit);

	/**
	 *
	 * @param namespace
	 * @param prefix
	 * @param key_start
	 * @param key_end
	 * @param offset
	 * @param limit
	 * @return
	 */
	Result<List<DataEntry>> getRangeOnlyValue(int namespace,
			Serializable prefix, Serializable key_start, Serializable key_end,
			int offset, int limit);

	/**
	 *
	 * @param namespace
	 * @param prefix
	 * @param key_start
	 * @param key_end
	 * @param offset
	 * @param limit
	 * @return
	 */

	Result<List<DataEntry>> getRangeOnlyKey(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset, int limit);

	/**
	 *
	 * @param namespace
	 * @param prefix
	 * @param key_start
	 * @param key_end
	 * @param offset
	 * @param limit
	 * @param reverse
	 * @return
	 */
	Result<List<DataEntry>> delRange(int namespace, Serializable prefix,
			Serializable key_start, Serializable key_end, int offset,
			int limit, boolean reverse);

	/**
	 *
	 * @param namespace
	 * @param key
	 * @param value
	 * @param defaultValue
	 * @param expireTime
	 * @return
	 */
	Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime);

	Result<Integer> incr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBound, int upperBound);

	/**
	 *
	 * @param namespace
	 * @param key
	 * @param value
	 * @param defaultValue
	 * @param expireTime
	 * @return
	 */
	Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime);

	Result<Integer> decr(int namespace, Serializable key, int value,
			int defaultValue, int expireTime, int lowBound, int upperBound);

	/**
	 *
	 * @param namespace
	 * @param key
	 * @param count
	 * @return
	 */
	ResultCode setCount(int namespace, Serializable key, int count);

	/**
	 *
	 * @param namespace
	 * @param key
	 * @param count
	 * @param version
	 * @param expireTime
	 * @return
	 */
	ResultCode setCount(int namespace, Serializable key, int count,
			int version, int expireTime);

	ResultCode addItems(int namespace, Serializable key,
			List<? extends Object> items, int maxCount, int version,
			int expireTime);

	Result<DataEntry> getItems(int namespace, Serializable key, int offset,
			int count);

	/**
	 * 这个removeItems真是迷一样的东西，offset难不成是整形？
	 * @param namespace
	 * @param key
	 * @param offset
	 * @param count
	 * @return
	 */
	ResultCode removeItems(int namespace, Serializable key, int offset,
			int count);

	Result<DataEntry> getAndRemove(int namespace, Serializable key, int offset,
			int count);

	Result<Integer> getItemCount(int namespace, Serializable key);

	/**
	 * 居然还造了锁？
	 * lock the key
	 *
	 * @param namespace
	 *            area, namespace the key belongs to
	 * @param key
	 *            the key to be locked
	 * @return SUCCESS, the lock operation was executed successfully TIMEOUT,
	 *         rpc timeout CONNERROR, some exceptions were occurred.
	 */
	ResultCode lock(int namespace, Serializable key);

	/**
	 * release the key which was locked
	 *
	 * @param namespace
	 *            area, namespace the key belongs to
	 * @param key
	 *            the key to be locked
	 * @return SUCCESS, the unlock operation was executed successfully TIMEOUT,
	 *         rpc timeout CONNERROR, some exceptions were occurred.
	 */
	ResultCode unlock(int namespace, Serializable key);

	/**
	 *
	 * @param namespace
	 * @param keys
	 * @return
	 */
	Result<List<Object>> mlock(int namespace, List<? extends Object> keys);

	/**
	 *failKeysMap 是啥？
	 * @param namespace
	 * @param keys
	 * @param failKeysMap
	 * @return
	 */
	Result<List<Object>> mlock(int namespace, List<? extends Object> keys,
			Map<Object, ResultCode> failKeysMap);

	/**
	 *
	 * @param namespace
	 * @param keys
	 * @return
	 */
	Result<List<Object>> munlock(int namespace, List<? extends Object> keys);

	/**
	 *
	 * @param namespace
	 * @param keys
	 * @param failKeysMap
	 * @return
	 */
	Result<List<Object>> munlock(int namespace, List<? extends Object> keys,
			Map<Object, ResultCode> failKeysMap);

	/**
	 * 这是啥意思？ withHeader 是啥意思？
	 * append value to key work with withHeader == false, and destination
	 * key/value must be byte[]/byte[]
	 *
	 * @param namespace
	 *            data's namespace
	 * @param key
	 *            data's key
	 * @param value
	 *            data's value
	 * @return return ResultCode.SUCCESS mean request success, otherwise fail
	 */
	ResultCode append(int namespace, byte[] key, byte[] value);

	/**
	 * get statistical infomation
	 *
	 * @param qtype
	 *            , info type
	 * @param groupName
	 *            , gorup name
	 * @param serverId
	 *            , server ip:port
	 * @return key-value maps
	 */
	Map<String, String> getStat(int qtype, String groupName, long serverId);
	public void setMaxFailCount(int failCount);

	public int getMaxFailCount();

	String getVersion();

	public Transcoder getTranscoder();

	public ResultCode lazyRemoveArea(int namespace);

	/**
	 * simpePrefixGets
	 */
	public Result<Map<Object, Result<DataEntry>>> simplePrefixGets(
			int namespace, Serializable pkey,
			List<? extends Serializable> subkeys);

	/**
	 * Read batch prefix key with multi key values.
	 *
	 * @param namespace
	 *            namespace for invalid operation
	 * @param pkeySkeyListMap
	 *            the set of prefix keys and suffix keys.
	 * @return SUCCESS, every thing is ok. TIMEOUT, rpc was timeout,
	 *         DATANOTEXIST, data not exist. CONNERROR, connection error
	 */
	public Result<Map<Object, Map<Object, Result<DataEntry>>>> mprefixGets(
			int namespace,
			Map<? extends Serializable, ? extends List<? extends Serializable>> pkeySkeyListMap);

	/**
	 * 这里应该是做了乐观锁吧
	 * @param namespace
	 * @param key
	 * @param value
	 * @param epoch
	 * @return
	 */
	public ResultCode compareAndPut(int namespace, Serializable key,
			Serializable value, short epoch);

	public ResultCode compareAndPut(int namespace, Serializable key,
			Serializable value, short epoch, int expireTime);

	/**
	 * group 是什么概念？
	 */
	public List<String> getGroupStatus(List<String> groups);

	public ResultCode setGroupStatus(String group, String status);

	public List<String> getNsStatus(List<String> groups);

	public ResultCode setNsStatus(String group, int namespace, String status);

	public String getTmpDownServer(String group);

	public List<String> getTmpDownServer(List<String> groups);

	public ResultCode resetServer(String group, String dataServer);

	public ResultCode resetDb(String group, String dataServer, int namespace);

	/**
	 * 这个不知道啥意思
	 */
	public boolean setFastdumpNamespaceGroupNum(int num);

	/**
	 * 这个不知道啥意思
	 */
	public int getMapedNamespace(int namespace);

	/**
	 * dumpNamespace是什么意思？
	 */
	public ResultCode mapNamespace(int namespace, int dumpNamespace);

	public int resetNamespace(int namespace);

	public String getConfigId();
}
