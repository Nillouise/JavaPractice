package com.taobao.tair.extend.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairBaseCallback;
import com.taobao.tair.TairCallback;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.extend.DataEntryList;
import com.taobao.tair.extend.DataEntryLong;
import com.taobao.tair.extend.DataEntrySimple;
import com.taobao.tair.extend.TairManagerList;
import com.taobao.tair.extend.packet.LeftOrRight;
import com.taobao.tair.extend.packet.list.request.RequestLIndexPacket;
import com.taobao.tair.extend.packet.list.request.RequestLLenPacket;
import com.taobao.tair.extend.packet.list.request.RequestLRPopPacket;
import com.taobao.tair.extend.packet.list.request.RequestLRPushLimitPacket;
import com.taobao.tair.extend.packet.list.request.RequestLRPushPacket;
import com.taobao.tair.extend.packet.list.request.RequestLRangePacket;
import com.taobao.tair.extend.packet.list.request.RequestLRemPacket;
import com.taobao.tair.extend.packet.list.request.RequestLTrimPacket;
import com.taobao.tair.extend.packet.list.response.ResponseLIndexPacket;
import com.taobao.tair.extend.packet.list.response.ResponseLLenPacket;
import com.taobao.tair.extend.packet.list.response.ResponseLRPopPacket;
import com.taobao.tair.extend.packet.list.response.ResponseLRPushPacket;
import com.taobao.tair.extend.packet.list.response.ResponseLRangePacket;
import com.taobao.tair.extend.packet.list.response.ResponseLRemPacket;
import com.taobao.tair.extend.packet.list.response.ResponseLTrimPacket;

public class TairListImpl implements TairManagerList {
	private TairManagerSession session;
	
	public TairListImpl(TairManagerSession s) {
		this.session = s;
	}
	
	private Result<DataEntryList> lrpop(short namespace,  Serializable key, 
			int count, short version, int expire, LeftOrRight lr) {
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		if (!session.isVaildCount(count)) {
			return new Result<DataEntryList>(ResultCode.COUNT_ZERO);
		}
	
		expire = TairUtil.getDuration(expire);
		
		RequestLRPopPacket request = session.helpNewRequest(RequestLRPopPacket.class, 
				namespace, key, version, expire);
		request.setLeftOrRight(lr);
		request.setCount(count);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryList>(resCode);
		
		ResponseLRPopPacket response = session.sendRequest(namespace, key, request, ResponseLRPopPacket.class);
		if (response == null) {
			return  new Result<DataEntryList> (ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
		
		resCode = ResultCode.valueOf(response.getResultCode());
		List<Object> values = response.getValues();
		DataEntryList entry = null;
		if (resCode == ResultCode.SUCCESS && values != null && values.size() > 0) {
			entry = new DataEntryList(key, values, response.getVersion());	
		}
		return new Result<DataEntryList>(resCode, entry);
	}
	
	private ResultCode lrpopAsync(short namespace,  Serializable key, 
			int count, short version, int expire, LeftOrRight lr, TairCallback cb) {
		if (!session.isValidNamespace(namespace)) {
			return ResultCode.NSERROR;
		}
		if (!session.isVaildCount(count)) {
			return ResultCode.COUNT_ZERO;
		}
	
		expire = TairUtil.getDuration(expire);
		
		RequestLRPopPacket request = session.helpNewRequest(RequestLRPopPacket.class, 
				namespace, key, version, expire);
		request.setLeftOrRight(lr);
		request.setCount(count);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb), SERVER_TYPE.DATA_SERVER);
	}
	
	private Result<DataEntryLong> lrpushLimit(short namespace, 
			Serializable key, List<? extends Serializable> vals, int maxcount,
			short version, int expire, LeftOrRight lr) {
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLRPushLimitPacket request = session.helpNewRequest(RequestLRPushLimitPacket.class, 
				namespace, key, version, expire);
		request.setMaxCount(maxcount);
		request.setLeftOrRight(lr);
		if (vals.size() > 8192) {
			return new Result<DataEntryLong>(ResultCode.TAIR_DATA_LEN_LIMIT);
		}
		for(int i = 0; i < vals.size(); i++) {
			request.addValue(vals.get(i));
		}
	
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);

		ResponseLRPushPacket response = session.sendRequest(namespace, key, request, ResponseLRPushPacket.class);
		if (response == null) {
			return  new Result<DataEntryLong>(ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
		return new Result<DataEntryLong>(ResultCode.valueOf(response.getResCode()),
				new DataEntryLong(key, response.getPushedNum()));
	}
	
	private Result<DataEntryLong> lrpush(short namespace, 
			Serializable key, List<? extends Serializable> vals, 
			short version, int expire, LeftOrRight lr) {
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLRPushPacket request = session.helpNewRequest(RequestLRPushPacket.class, 
				namespace, key, version, expire);
		request.setLeftOrRight(lr);
		if (vals.size() > 8192) {
			return new Result<DataEntryLong>(ResultCode.TAIR_DATA_LEN_LIMIT);
		}
		for(int i = 0; i < vals.size(); i++) {
			request.addValue(vals.get(i));
		}
	
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);

		ResponseLRPushPacket response = session.sendRequest(namespace, key, request, ResponseLRPushPacket.class);
		if (response == null) {
			return  new Result<DataEntryLong>(ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
		return new Result<DataEntryLong>(ResultCode.valueOf(response.getResCode()),
				new DataEntryLong(key, response.getPushedNum()));
	}

	private ResultCode lrpushAsync(short namespace, 
			Serializable key, List<? extends Serializable> vals, 
			short version, int expire, LeftOrRight lr, TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLRPushPacket request = session.helpNewRequest(RequestLRPushPacket.class, 
				namespace, key, version, expire);
		request.setLeftOrRight(lr);
		if (vals.size() > 8192) {
			return ResultCode.TAIR_DATA_LEN_LIMIT;
		}
		for(int i = 0; i < vals.size(); i++) {
			request.addValue(vals.get(i));
		}
	
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public Result<DataEntryLong> llen(short namespace, Serializable key) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestLLenPacket request = session.helpNewRequest(RequestLLenPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_VERSION);
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseLLenPacket response = session.sendRequest(namespace, key, request, ResponseLLenPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);

		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, response.getValue())
		);
	}

	public Result<DataEntryLong> lrem(short namespace, Serializable key,
			Serializable value, int count, short version, int expire) {
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		if (count == 0) {
			return new Result<DataEntryLong>(ResultCode.COUNT_ZERO);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLRemPacket request = session.helpNewRequest(RequestLRemPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		request.setCount(count);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseLRemPacket response = session.sendRequest(namespace, key, request, ResponseLRemPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
	
		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, value, response.getValue())
		);
	}

	public ResultCode lremAsync(short namespace, Serializable key,
			Serializable value, int count, short version, int expire,
			TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		if (count == 0) {
			return ResultCode.COUNT_ZERO;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLRemPacket request = session.helpNewRequest(RequestLRemPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		request.setCount(count);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public ResultCode ltrim(short namespace, Serializable key, 
			int start, int end, short version, int expire) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLTrimPacket request = session.helpNewRequest(RequestLTrimPacket.class, 
				namespace, key, version, expire);
		request.setStart(start);
		request.setEnd(end);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		ResponseLTrimPacket response = session.sendRequest(namespace, key, request, ResponseLTrimPacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		}
		session.checkConfigVersion(response);	
		
		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode ltrimAsync(short namespace, Serializable key, int start,
			int end, short version, int expire, TairCallback cb) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestLTrimPacket request = session.helpNewRequest(RequestLTrimPacket.class, 
				namespace, key, version, expire);
		request.setStart(start);
		request.setEnd(end);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
				
	}
	
	public Result<DataEntrySimple> lindex(short namespace, Serializable key, int index) {		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntrySimple>(ResultCode.NSERROR);
		}

		RequestLIndexPacket request = session.helpNewRequest(RequestLIndexPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_VERSION);
		request.setIndex(index);
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntrySimple>(resCode);
		
		ResponseLIndexPacket response = session.sendRequest(namespace, key, request, ResponseLIndexPacket.class);
		if (response == null) {
			return new Result<DataEntrySimple>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);	
		
		return new Result<DataEntrySimple>(ResultCode.valueOf(
					response.getResultCode()), 
					new DataEntrySimple(key, response.getValue(), response.getVersion())
		);
	}
	
	public Result<DataEntryList> lrange(short namespace, Serializable key,
			int start, int end) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		
		RequestLRangePacket request = session.helpNewRequest(RequestLRangePacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_VERSION);
		request.setStart(start);
		request.setEnd(end);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryList>(resCode);
		
		ResponseLRangePacket response = session.sendRequest(namespace, key, request, ResponseLRangePacket.class);
		if (response == null) {
			return new Result<DataEntryList>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);	
		
		return new Result<DataEntryList>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryList(key, response.getValues(), response.getVersion())
		);
	}
	
	public Result<DataEntryLong> rpush(short namespace, 
			Serializable key, Serializable val, 
			short version, int expire) {
		List<Serializable> vals = new ArrayList<Serializable>();
		vals.add(val);
		return lrpush(namespace, key, vals, 
					  version, expire, LeftOrRight.IS_R);
	}
	
	public Result<DataEntryLong> rpushLimit(short namespace,  
			Serializable key, Serializable value, int maxcount,
			short version, int expire) {
		List<Serializable> vals = new ArrayList<Serializable>();
		vals.add(value);
		return lrpushLimit(namespace, key, vals, maxcount,
					  version, expire, LeftOrRight.IS_R);
	}
	
	public ResultCode rpushAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, TairCallback cb) {
		List<Serializable> vals = new ArrayList<Serializable>();
		vals.add(value);
		return lrpushAsync(namespace, key, vals, version, expire, LeftOrRight.IS_R, cb);
	}
	
	public Result<DataEntryLong> rpush(short namespace, 
			Serializable key, List<? extends Serializable> vals, 
			short version, int expire) {
		return lrpush(namespace, key, vals, 
					  version, expire, LeftOrRight.IS_R);
	}
	
	public Result<DataEntryLong> rpushLimit(short namespace, 
			Serializable key, List<? extends Serializable> vals, int maxcount,
			short version, int expire) {
		return lrpushLimit(namespace, key, vals, maxcount,
					  version, expire, LeftOrRight.IS_R);
	}
	
	public ResultCode rpushAsync(short namespace, Serializable key,
			List<? extends Serializable> vals, short version, int expire, TairCallback cb) {
		return lrpushAsync(namespace, key, vals, version, expire, LeftOrRight.IS_R, cb);
	}
	
	public Result<DataEntryLong> lpush(short namespace,  
			Serializable key, Serializable value, 
			short version, int expire) {
		List<Serializable> vals = new ArrayList<Serializable>();
		vals.add(value);
		return lrpush(namespace, key, vals, 
					  version, expire, LeftOrRight.IS_L);
	}
	
	public Result<DataEntryLong> lpushLimit(short namespace,  
			Serializable key, Serializable value, int maxcount,
			short version, int expire) {
		List<Serializable> vals = new ArrayList<Serializable>();
		vals.add(value);
		return lrpushLimit(namespace, key, vals, maxcount,
					  version, expire, LeftOrRight.IS_L);
	}
	
	public ResultCode lpushAsync(short namespace, Serializable key,
			Serializable value, short version, int expire, TairCallback cb) {
		List<Serializable> vals = new ArrayList<Serializable>();
		vals.add(value);
		return lrpushAsync(namespace, key, vals, version, expire, LeftOrRight.IS_L, cb);
	}
	
	public Result<DataEntryLong> lpushLimit(short namespace, 
			Serializable key, List<? extends Serializable> vals, int maxcount,
			short version, int expire) {
		return lrpushLimit(namespace, key, vals, maxcount,
					  version, expire, LeftOrRight.IS_L);
	}
	
	public Result<DataEntryLong> lpush(short namespace, 
			Serializable key, List<? extends Serializable> vals, 
			short version, int expire) {
		return lrpush(namespace, key, vals, 
					  version, expire, LeftOrRight.IS_L);
	}
	
	public ResultCode lpushAsync(short namespace, Serializable key,
			List<? extends Serializable> vals, short version, int expire, TairCallback cb) {
		return lrpushAsync(namespace, key, vals, version, expire, LeftOrRight.IS_L, cb);
	}
	
	public Result<DataEntryList> lpop(short namespace, Serializable key,
			int count, short version, int expire) {
		return lrpop(namespace, key, count, 
				     version, expire, LeftOrRight.IS_L);
	}

	public ResultCode lpopAsync(short namespace, Serializable key,
			int count, short version, int expire, TairCallback cb) {
		return lrpopAsync(namespace, key, count, version, expire, LeftOrRight.IS_L, cb);
	}
	
	public Result<DataEntryList> rpop(short namespace, Serializable key,
			int count, short version, int expire) {
		return lrpop(namespace, key, count, 
					 version, expire, LeftOrRight.IS_R);
	}
	
	public ResultCode rpopAsync(short namespace, Serializable key,
			int count, short version, int expire, TairCallback cb) {
		return lrpopAsync(namespace, key, count, version, expire, LeftOrRight.IS_R, cb);
	}
}
