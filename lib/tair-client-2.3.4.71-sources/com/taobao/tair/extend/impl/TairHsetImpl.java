package com.taobao.tair.extend.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairBaseCallback;
import com.taobao.tair.TairCallback;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.extend.DataEntryList;
import com.taobao.tair.extend.DataEntryLong;
import com.taobao.tair.extend.DataEntryMap;
import com.taobao.tair.extend.DataEntrySimple;
import com.taobao.tair.extend.TairManagerHset;
import com.taobao.tair.extend.packet.hset.request.RequestHDelPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHExistsPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHGetPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHGetallPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHIncrbyPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHLenPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHMgetPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHMsetPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHSetPacket;
import com.taobao.tair.extend.packet.hset.request.RequestHValsPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHDelPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHGetPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHGetallPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHIncrbyPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHLenPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHMgetPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHMsetPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHSetPacket;
import com.taobao.tair.extend.packet.hset.response.ResponseHValsPacket;
import com.taobao.tair.packet.ReturnPacket;

public class TairHsetImpl implements TairManagerHset {
	private TairManagerSession session;
	
	public TairHsetImpl(TairManagerSession s) {
		this.session = s;
	}
	
	public Result<DataEntryMap> hgetall(short namespace, Serializable key) {
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntryMap>(ResultCode.NSERROR);
		}
		
		RequestHGetallPacket request = session.helpNewRequest(RequestHGetallPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryMap>(resCode);
		
		ResponseHGetallPacket response = session.sendRequest(namespace, key, request, ResponseHGetallPacket.class);
		if (response == null) {
			return  new Result<DataEntryMap> (ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
	
		resCode = ResultCode.valueOf(response.getResultCode());
		Map<Object, Object> values = response.getValues();
		DataEntryMap entry = null;
		if (resCode == ResultCode.SUCCESS && values != null && values.size() > 0) {
			entry = new DataEntryMap(key, values, response.getVersion());	
		}
		
		return new Result<DataEntryMap>(resCode, entry);
	}

	public Result<DataEntryLong> hincrby(short namespace, Serializable key, 
			Serializable field, int addvalue, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHIncrbyPacket request = session.helpNewRequest(RequestHIncrbyPacket.class, 
				namespace, key, version, expire);
		request.setField(field);
		request.setAddValue(addvalue);
	
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseHIncrbyPacket response = session.sendRequest(namespace, key, request, ResponseHIncrbyPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);

		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, field, response.getValue())
		);
	}

	public ResultCode hincrbyAsync(short namespace,
			Serializable key, Serializable field, int addvalue, short version,
			int expire, TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHIncrbyPacket request = session.helpNewRequest(RequestHIncrbyPacket.class, 
				namespace, key, version, expire);
		request.setField(field);
		request.setAddValue(addvalue);
	
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) {
			return resCode;
		}
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public Result<DataEntryMap> hmset(short namespace, Serializable key,
			Map<? extends Serializable, ? extends Serializable> field_values,
			 short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryMap>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHMsetPacket request = session.helpNewRequest(RequestHMsetPacket.class, 
				namespace, key, version, expire);
		for(Entry<? extends Serializable, ? extends Serializable> fv : field_values.entrySet()) {
			if (fv.getKey() == null || fv.getValue() == null) {
				return new Result<DataEntryMap>(ResultCode.SERIALIZEERROR);
			}
			request.addFieldValue(fv.getKey(), fv.getValue());
		}

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryMap>(resCode);

		ResponseHMsetPacket response = session.sendRequest(namespace, key, request, ResponseHMsetPacket.class);
		if (response == null) {
			return new Result<DataEntryMap>(ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
		
		if (response.getValue() == field_values.size()) {
			return new Result<DataEntryMap>(ResultCode.valueOf(response.getResultCode()));
		} else {
			int len = response.getValue();
			Map<Object, Object> map = request.getValues();
			Map<Object, Object> last = new HashMap<Object, Object>();
			Iterator<Entry<Object, Object>> it = map.entrySet().iterator();
			while(it.hasNext() && len > 0) {
				Entry<Object, Object> entry = it.next();
				last.put(entry.getKey(), entry.getValue());
				len--;
			}
			DataEntryMap out = new DataEntryMap(key, last, (short)0);
			return new Result<DataEntryMap>(ResultCode.valueOf(response.getResultCode()), out);
		}
	}

	public ResultCode hmsetAsync(short namespace, Serializable key,
			Map<? extends Serializable, ? extends Serializable> field_values,
			short version, int expire, TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHMsetPacket request = session.helpNewRequest(RequestHMsetPacket.class, 
				namespace, key, version, expire);
		for(Entry<? extends Serializable, ? extends Serializable> fv : field_values.entrySet()) {
			if (fv.getKey() == null || fv.getValue() == null) {
				return ResultCode.SERIALIZEERROR;
			}
			request.addFieldValue(fv.getKey(), fv.getValue());
		}

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public ResultCode hset(short namespace, Serializable key, Serializable field,
			Serializable value, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHSetPacket request = session.helpNewRequest(RequestHSetPacket.class, 
				namespace, key, version, expire);
		request.setField(field);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		ResponseHSetPacket response = session.sendRequest(namespace, key, request, ResponseHSetPacket.class);
		if (response == null) {
			return  ResultCode.CONNERROR;
		} 
		session.checkConfigVersion(response);
		
		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode hsetAsync(short namespace, Serializable key,
			Serializable field, Serializable value, short version, int expire,
			TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHSetPacket request = session.helpNewRequest(RequestHSetPacket.class, 
				namespace, key, version, expire);
		request.setField(field);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public Result<DataEntrySimple> hget(short namespace, Serializable key,
			Serializable field) {
		
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntrySimple>(ResultCode.NSERROR);
		}
		
		RequestHGetPacket request = session.helpNewRequest(RequestHGetPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setField(field);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntrySimple>(resCode);

		ResponseHGetPacket response = session.sendRequest(namespace, key, request, ResponseHGetPacket.class);
		if (response == null) {
			return  new Result<DataEntrySimple> (ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);

		return new Result<DataEntrySimple>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntrySimple(key, field, response.getValue(), response.getVersion())
		);
	}

	public Result<DataEntryMap> hmget(short namespace, Serializable key,
			List<? extends Serializable> fields) {
		
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntryMap>(ResultCode.NSERROR);
		}
		
		RequestHMgetPacket request = session.helpNewRequest(RequestHMgetPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		for(Serializable field : fields) {
			request.addField(field);
		}

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryMap>(resCode);

		ResponseHMgetPacket response = session.sendRequest(namespace, key, request, ResponseHMgetPacket.class);
		if (response == null) {
			return  new Result<DataEntryMap> (ResultCode.CONNERROR);
		} 
		session.checkConfigVersion(response);
		
		resCode = ResultCode.valueOf(response.getResultCode());
		
		if (response.getValues().size() != fields.size()) {
			return new Result<DataEntryMap>(resCode);
		}
		
		Map<Object, Object> map = new HashMap<Object, Object>();
		for(int i = 0; i < fields.size(); i++) {
			map.put(fields.get(i), response.getValues().get(i));
		}
		DataEntryMap entry = new DataEntryMap(key, map, response.getVersion());	

		return new Result<DataEntryMap>(resCode, entry);
	}

	public Result<DataEntryList> hvals(short namespace, Serializable key) {
		
		if (!session.isValidNamespace(namespace)) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		
		RequestHValsPacket request = session.helpNewRequest(RequestHValsPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryList>(resCode);

		ResponseHValsPacket response = session.sendRequest(namespace, key, request, ResponseHValsPacket.class);
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

	public Result<DataEntryLong> hlen(short namespace, Serializable key) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestHLenPacket request = session.helpNewRequest(RequestHLenPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseHLenPacket response = session.sendRequest(namespace, key, request, ResponseHLenPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);

		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, response.getValue())
		);
	}

	public ResultCode hdel(short namespace, Serializable key,
			Serializable field, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHDelPacket request = session.helpNewRequest(RequestHDelPacket.class, 
				namespace, key, version, expire);
		request.setField(field);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		ResponseHDelPacket response = session.sendRequest(namespace, key, request, ResponseHDelPacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		}
		session.checkConfigVersion(response);
		
		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode hdelAsync(short namespace, Serializable key,
			Serializable field, short version, int expire, TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestHDelPacket request = session.helpNewRequest(RequestHDelPacket.class, 
				namespace, key, version, expire);
		request.setField(field);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}

	public ResultCode hexists(short namespace, Serializable key,
			Serializable field) {
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		RequestHExistsPacket request = session.helpNewRequest(RequestHExistsPacket.class,	
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setField(field);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		ReturnPacket response = session.sendRequest(namespace, key, request, ReturnPacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		}
		session.checkConfigVersion(response);
	
		return ResultCode.valueOf(response.getCode());
	}
}
