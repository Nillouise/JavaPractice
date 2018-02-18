/**
 * 
 */
package com.taobao.tair.extend;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairCallback;



/**
 * Tair 扩展接口，支持Set系列操作
 * 
 * @author YeXiang
 * 
 */
public interface TairManagerSet {
	
	
	/**
	 * 对对应的set插入value
	 * @param namespace 对应的namespace的名字
	 * @param key 对应的Set的名字
	 * @param value 要插入的value
	 * @param version sadd的带版本验证版本，版本不对，返回错误
	 * @param expire 超时时间  功能参见expire API
	 * @return 返回成功或者失败
	 */
	public ResultCode sadd(short namespace, Serializable key, Serializable value,
			short version, int expire);
	
	public ResultCode saddAsync(short namespace, Serializable key, Serializable value,
			short version, int expire, TairCallback cb);

	public ResultCode msadd(short namespace, Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
			int expire);
	
	public ResultCode msaddAsync(short namespace, Map<? extends Serializable, ? extends Set<? extends Serializable>> kvs,
			int expire, TairCallback cb);

	/**
	 * 对对应的set删除value
	 * @param namespace 对应的namespace的名字
	 * @param key 对应的Set的名字
	 * @param value 要插入的value
	 * @param version sadd的带版本验证版本，版本不对，返回错误
	 * @param expire 超时时间  功能参见expire API
	 * @return 返回成功或者失败
	 */
	public ResultCode srem(short namespace, Serializable key, Serializable value,
			short version, int expire);
	
	public ResultCode sremAsync(short namespace, Serializable key, Serializable value,
			short version, int expire, TairCallback cb);
	
	public ResultCode msrem(short namespace, Map<? extends Serializable,
			? extends Set<? extends Serializable>> kvs, int expire);
	
	public ResultCode msremAsync(short namespace, Map<? extends Serializable,
			? extends Set<? extends Serializable>> kvs, int expire, TairCallback cb);

	/**
	 * 获取对应Set的value的个数
	 * @param namespace 对应的namespace的名字
	 * @param key 对应的Set的名字
	 * @return 返回一个DataEntryLong key为输入的key,value为long型的个数  否则返回失败
	 */
	public Result<DataEntryLong> scard(short namespace, Serializable key);
	
	/**
	 * 获取对应Set的所有value
	 * @param namespace 对应的namespace的名字
	 * @param key 对应的Set的名字
	 * @return 返回返回一个DataEntrySet,key为输入的key,value为Set<Object>表示返回的集合，
	 * 			否则失败返回错误码
	 */
	public Result<DataEntrySet> smembers(short namespace, Serializable key);
	
	public Result<Set<DataEntrySet>> msmembers(short namespace, List<? extends Serializable> keys);
	
	/**
	 * 随机从Set中去除掉一个value
	 * @param namespace 对应的namespace的名字
	 * @param key 对应的Set的名字
	 * @param version spop的带版本验证版本，版本不对，返回错误 
	 * @param expire 超时时间  功能参见expire API
	 * @return 返回一个DataEntrySimple, key为输入的key,value为去除的object
	 */
	public Result<DataEntrySimple> spop(short namespace, Serializable key, short version,
			int expire);
	
	public ResultCode spopAsync(short namespace, Serializable key, short version,
			int expire, TairCallback cb);
}
