package com.taobao.tair.extend.impl;

import java.io.Serializable;

import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairBaseCallback;
import com.taobao.tair.TairCallback;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.extend.DataEntryDouble;
import com.taobao.tair.extend.DataEntryList;
import com.taobao.tair.extend.DataEntryLong;
import com.taobao.tair.extend.TairManagerZset;
import com.taobao.tair.extend.packet.zet.request.RequestGenericZRangeByScorePacket;
import com.taobao.tair.extend.packet.zet.request.RequestZAddPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZCardPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZCountPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZIncrbyPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRangePacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRangebyscorePacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRankPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRemPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRemrangebyrankPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRemrangebyscorePacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRevrangePacket;
import com.taobao.tair.extend.packet.zet.request.RequestZRevrankPacket;
import com.taobao.tair.extend.packet.zet.request.RequestZScorePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseGenericZRangeByScorePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZAddPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZCardPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZCountPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZIncrbyPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRangePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRangeWithScorePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRangebyscorePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRankPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRemPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRemrangebyrankPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRemrangebyscorePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRevrangePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRevrangeWithScorePacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZRevrankPacket;
import com.taobao.tair.extend.packet.zet.response.ResponseZScorePacket;

public class TairZsetImpl implements TairManagerZset {
	private TairManagerSession session;
	
	public TairZsetImpl(TairManagerSession s) {
		this.session = s;
	}
	
	public Result<DataEntryDouble> zscore(short namespace, Serializable key,
			Serializable value) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryDouble>(ResultCode.NSERROR);
		}
		
		RequestZScorePacket request = session.helpNewRequest(RequestZScorePacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setvalue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryDouble>(resCode);
		
		ResponseZScorePacket response = session.sendRequest(namespace, key, request, ResponseZScorePacket.class);
		if (response == null) {
			return new Result<DataEntryDouble>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryDouble>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryDouble(key, value, response.getValue())
		);		
	}

	public Result<DataEntryList> zrange(short namespace, Serializable key,
			int start, int end, boolean withscore) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		
		RequestZRangePacket request = session.helpNewRequest(RequestZRangePacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setStart(start);
		request.setEnd(end);
		request.setWithScore(withscore);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess())
			return new Result<DataEntryList>(resCode);
		ResponseZRangePacket response = null;
		if (withscore == false) {
			response = session.sendRequest(namespace, key, request,
					ResponseZRangePacket.class);
		} else {
			response = session.sendRequest(namespace, key, request, 
					ResponseZRangeWithScorePacket.class);
		}
		if (response == null) {
			return new Result<DataEntryList>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);

		return new Result<DataEntryList>(ResultCode.valueOf(response
				.getResultCode()), new DataEntryList(key,
				response.getValues(), response.getVersion()));
	}

	public Result<DataEntryList> zrevrange(short namespace, Serializable key,
			int start, int end, boolean withscore) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		
		RequestZRevrangePacket request = session.helpNewRequest(RequestZRevrangePacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setStart(start);
		request.setEnd(end);
		request.setWithScore(withscore);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess())
			return new Result<DataEntryList>(resCode);
		ResponseZRevrangePacket response = null;
		if (withscore == false) {
			response = session.sendRequest(namespace, key, request,
					ResponseZRevrangePacket.class);
		} else {
			response = session.sendRequest(namespace, key, request, 
					ResponseZRevrangeWithScorePacket.class);
		}
		if (response == null) {
			return new Result<DataEntryList>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);

		return new Result<DataEntryList>(ResultCode.valueOf(response
				.getResultCode()), new DataEntryList(key,
				response.getValues(), response.getVersion()));
	}

	private Result<DataEntryList> genericZrangebyscore(short namespace,
			Serializable key, double start, double end, boolean reverse, int limit, boolean withscore) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		
		RequestGenericZRangeByScorePacket request =
				session.helpNewRequest(RequestGenericZRangeByScorePacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setStartScore(start);
		request.setEndScore(end);
		request.setWithScore(withscore);
		request.setLimit(limit);
		request.setReverse(reverse);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess())
			return new Result<DataEntryList>(resCode);
		ResponseGenericZRangeByScorePacket response = session.sendRequest(namespace, key, request,
				ResponseGenericZRangeByScorePacket.class);
		if (response == null) {
			return new Result<DataEntryList>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);

		return new Result<DataEntryList>(ResultCode.valueOf(response
				.getResultCode()), new DataEntryList(key,
				response.getValues(), response.getVersion()));
	}
	
	public Result<DataEntryList> zrangebyscore(short namespace,
			Serializable key, double min, double max, int limit, boolean withsocre) {
		return genericZrangebyscore(namespace, key, min, max, true, limit, withsocre);
	}
	
	public Result<DataEntryList> zrevrangebyscore(short namespace,
			Serializable key, double max, double min, int limit, boolean withscore) {
		return genericZrangebyscore(namespace, key, max, min, false, limit, withscore);
	}
	
	public Result<DataEntryList> zrangebyscore(short namespace,
			Serializable key, double min, double max) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryList>(ResultCode.NSERROR);
		}
		
		RequestZRangebyscorePacket request = session.helpNewRequest(RequestZRangebyscorePacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setStartScore(min);
		request.setEndScore(max);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryList>(resCode);
		
		ResponseZRangebyscorePacket response = session.sendRequest(namespace, key, request,
				ResponseZRangebyscorePacket.class);
		if (response == null) {
			return new Result<DataEntryList>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryList>(
					ResultCode.valueOf(response.getResultCode()), 
					new DataEntryList(key, response.getValues(), response.getVersion())
		);
	}

	public ResultCode zadd(short namespace, Serializable key,
			Serializable value, double score, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZAddPacket request = session.helpNewRequest(RequestZAddPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		request.setScore(score);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		ResponseZAddPacket response = session.sendRequest(namespace, key, request, ResponseZAddPacket.class);
		if (response == null) {
			return  ResultCode.CONNERROR;
		} 
		session.checkConfigVersion(response);
		
		return ResultCode.valueOf(response.getCode());
	}

	public ResultCode zaddAsync(short namespace, Serializable key,
			Serializable value, double score, short version, int expire,
			TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZAddPacket request = session.helpNewRequest(RequestZAddPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		request.setScore(score);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;

		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public Result<DataEntryLong> zrank(short namespace, Serializable key,
			Serializable value) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestZRankPacket request = session.helpNewRequest(RequestZRankPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		request.setNamespace(namespace);
		request.setKey(key);
		request.setvalue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseZRankPacket response = session.sendRequest(namespace, key, request, ResponseZRankPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
					ResultCode.valueOf(response.getResultCode()), 
					new DataEntryLong(key, value, response.getValue())
		);
	}

	public Result<DataEntryLong> zcard(short namespace, Serializable key) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestZCardPacket request = session.helpNewRequest(RequestZCardPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseZCardPacket response = session.sendRequest(namespace, key, request, ResponseZCardPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
					ResultCode.valueOf(response.getResultCode()), 
					new DataEntryLong(key, response.getValue())
		);
	}
	
	public Result<DataEntryLong> zrevrank(short namespace, Serializable key,
			Serializable value) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestZRevrankPacket request = session.helpNewRequest(RequestZRevrankPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setvalue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseZRevrankPacket response = session.sendRequest(namespace, key, request, ResponseZRevrankPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
				ResultCode.valueOf(response.getResultCode()), 
				new DataEntryLong(key, value, response.getValue())
		);
	}

	public ResultCode zrem(short namespace, Serializable key, Serializable value,
			short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZRemPacket request = session.helpNewRequest(RequestZRemPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		ResponseZRemPacket response = session.sendRequest(namespace, key, request, ResponseZRemPacket.class);
		if (response == null) {
			return ResultCode.CONNERROR;
		}
		session.checkConfigVersion(response);
		
		return ResultCode.valueOf(response.getCode());
	}
	
	public ResultCode zremAsync(short namespace, Serializable key,
			Serializable value, short version, int expire,
			TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZRemPacket request = session.helpNewRequest(RequestZRemPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}

	public Result<DataEntryLong> zremrangebyscore(short namespace, Serializable key,
			double start, double end, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZRemrangebyscorePacket request = session.helpNewRequest(RequestZRemrangebyscorePacket.class, 
				namespace, key, version, expire);
		request.setStartScore(start);
		request.setEndScore(end);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseZRemrangebyscorePacket response = session.sendRequest(namespace, key, request,
				ResponseZRemrangebyscorePacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
					ResultCode.valueOf(response.getResultCode()), 
					new DataEntryLong(key, response.getValue())
		);
	}

	public ResultCode zremrangebyscoreAsync(short namespace,
			Serializable key, double start, double end,
			short version, int expire, TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZRemrangebyscorePacket request = session.helpNewRequest(RequestZRemrangebyscorePacket.class, 
				namespace, key, version, expire);
		request.setStartScore(start);
		request.setEndScore(end);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public Result<DataEntryLong> zremrangebyrank(short namespace, Serializable key,
			int start, int end, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZRemrangebyrankPacket request = session.helpNewRequest(RequestZRemrangebyrankPacket.class, 
				namespace, key, version, expire);
		request.setStart(start);
		request.setEnd(end);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseZRemrangebyrankPacket response = session.sendRequest(namespace, key, request,
				ResponseZRemrangebyrankPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
				 	ResultCode.valueOf(response.getResultCode()), 
				 	new DataEntryLong(key, response.getValue())
		);
	}

	public ResultCode zremrangebyrankAsync(short namespace,
			Serializable key, int start, int end,
			short version, int expire, TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZRemrangebyrankPacket request = session.helpNewRequest(RequestZRemrangebyrankPacket.class, 
				namespace, key, version, expire);
		request.setStart(start);
		request.setEnd(end);

		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
	
	public Result<DataEntryLong> zcount(short namespace, Serializable key,
			double start, double end) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryLong>(ResultCode.NSERROR);
		}
		
		RequestZCountPacket request = session.helpNewRequest(RequestZCountPacket.class, 
				namespace, key, TairConstant.NOT_CARE_VERSION, TairConstant.NOT_CARE_EXPIRE);
		request.setStartScore(start);
		request.setEndScore(end);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryLong>(resCode);
		
		ResponseZCountPacket response = session.sendRequest(namespace, key, request, ResponseZCountPacket.class);
		if (response == null) {
			return new Result<DataEntryLong>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryLong>(
					ResultCode.valueOf(response.getResultCode()), 
					new DataEntryLong(key, response.getValue())
		);
	}
	
	public Result<DataEntryDouble> zincrby(short namespace, Serializable key,
			Serializable value, double addvalue, short version, int expire) {
		
		if (session.isValidNamespace(namespace) == false) {
			return new Result<DataEntryDouble>(ResultCode.NSERROR);
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZIncrbyPacket request = session.helpNewRequest(RequestZIncrbyPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		request.setAddValue(addvalue);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return new Result<DataEntryDouble>(resCode);
		
		ResponseZIncrbyPacket response = session.sendRequest(namespace, key, request, ResponseZIncrbyPacket.class);
		if (response == null) {
			return new Result<DataEntryDouble>(ResultCode.CONNERROR);
		}
		session.checkConfigVersion(response);
		
		return new Result<DataEntryDouble>(
					ResultCode.valueOf(response.getResultCode()), 
					new DataEntryDouble(key, response.getValue())
		);
	}

	public ResultCode zincrbyAsync(short namespace,
			Serializable key, Serializable value, double addvalue,
			short version, int expire, TairCallback cb) {
		
		if (session.isValidNamespace(namespace) == false) {
			return ResultCode.NSERROR;
		}
		
		expire = TairUtil.getDuration(expire);
		
		RequestZIncrbyPacket request = session.helpNewRequest(RequestZIncrbyPacket.class, 
				namespace, key, version, expire);
		request.setValue(value);
		request.setAddValue(addvalue);
		
		ResultCode resCode = session.tryEncode(request);
		if (!resCode.isSuccess()) 
			return resCode;
		
		return session.sendAsyncRequest(namespace, key, request, false, new TairBaseCallback(cb),
				SERVER_TYPE.DATA_SERVER);
	}
}
