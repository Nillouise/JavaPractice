/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair;

import java.util.HashMap;
import java.util.Map;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.etc.TairConstant;

/**
 * resutlcode
 */
public class ResultCode implements Serializable{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(ResultCode.class);
	//must before result codes
	public static final Map<Integer, ResultCode> codeMap = new HashMap<Integer, ResultCode>();

	public static final ResultCode SUCCESS = createResultCode(0, "success");
	public static final ResultCode DATANOTEXSITS = createResultCode(1, "data not exist");
	public static final ResultCode MEMBERNOTEXISTS = createResultCode(2, "member not exist");
	public static final ResultCode CONNERROR = createResultCode(-1, "connection error or timeout");
	public static final ResultCode UNKNOW = createResultCode(-2, "unknow error");
	public static final ResultCode KEYORVALUEISNULL = createResultCode(-3, "key or value is null");;
	public static final ResultCode KEYTOLARGE = createResultCode(-5, "key length error");
	public static final ResultCode VALUETOLARGE = createResultCode(-6, "value length error");
	public static final ResultCode NSERROR = createResultCode(-7, "namsepace range error, should between 0 ~ " + TairConstant.NAMESPACE_MAX);
	public static final ResultCode PARTSUCC = createResultCode(-10, "partly success");
	public static final ResultCode ASYNCERR = createResultCode(-11, "async call failed, pool maybe full");
	public static final ResultCode CLIENTHANDLERERR = createResultCode(-12, "multi cluter handler is null");
	public static final ResultCode OVERFLOW = createResultCode(-13, "namespace overflow");
	public static final ResultCode CLIENT_NOT_INITED = createResultCode(-14, "tair client not inited");

	public static final ResultCode SERVERERROR = createResultCode(-3999, "server error");
	public static final ResultCode VERERROR = createResultCode(-3997, "version error");
	public static final ResultCode TYPENOTMATCH = createResultCode(-3996, "type not match");
	public static final ResultCode PLUGINERROR = createResultCode(-3995, "plugin error");
	public static final ResultCode SERIALIZEERROR = createResultCode(-3994, "serialize error");
	public static final ResultCode ITEMEMPRY = createResultCode(-3993, "item empty");
	public static final ResultCode OUTOFRANGE = createResultCode(-3992, "item out of range");
	public static final ResultCode ITEMSIZEERROR = createResultCode(-3991, "item size error");
	public static final ResultCode SENDFAILED = createResultCode(-3990, "send packet failed");
	public static final ResultCode TIMEOUT = createResultCode(-3989, "timeout");
	public static final ResultCode DATAEXPIRED = createResultCode(-3988, "data expired");
	public static final ResultCode SERVERCANTWORK = createResultCode(-3987, "server can not work");
	public static final ResultCode WRITENOTONMASTER = createResultCode(-3986, "write not on master");
	public static final ResultCode DUPLICATEBUSY = createResultCode(-3985, "duplicate busy");
	public static final ResultCode MIGRATEBUSY = createResultCode(-3984, "migrate busy");
	public static final ResultCode INVALIDARG = createResultCode(-3982, "invalid argument");
	public static final ResultCode CANNT_OVERRIDE = createResultCode(-3981, "can not override");
	public static final ResultCode COUNT_BOUNDS = createResultCode(-3980, "count reach bound");
	public static final ResultCode COUNT_ZERO = createResultCode(-3979, "count reach zero");
	public static final ResultCode COUNT_NOTFOUND = createResultCode(-3978, "count not exist");
	public static final ResultCode INVAL_CONN_ERROR = createResultCode(-3970, "invalid server can't connect to dataserver");
	public static final ResultCode ITEM_HIDDEN = createResultCode(-3969, "item is hidden");
	public static final ResultCode ITEM_NEGLECTED = createResultCode(-3966, "item is neglected");
	public static final ResultCode QUEUE_OVERFLOWED = createResultCode(-3968, "server-side task queue overflowed");
	public static final ResultCode SHOULD_PROXY = createResultCode(-3967, "key should proxy");

	public static final ResultCode LOCK_EXIST = createResultCode(-3975, "lock already exist");
	public static final ResultCode LOCK_NOT_EXIST = createResultCode(-3974, "lock not exist");

	public static final ResultCode NAMESPACE_EXISTED = createResultCode(-4204, "namespace already exists");
	public static final ResultCode COUNTER_OUT_OF_RANGE = createResultCode(-5114, "counter out of the range");
	public static final ResultCode MODIFYTIMEERROR = createResultCode(-5115, "modify time smaller than server time");

	public static final ResultCode TARGET_NOT_NUMBER = createResultCode(-20001, "target not number");
	public static final ResultCode TARGET_NOT_INTEGER = createResultCode(-20002, "target not integer");
	public static final ResultCode TARGET_NOT_DOUBLE = createResultCode(-20003, "target not double");
	public static final ResultCode TARGET_ALREADY_EXIST = createResultCode(-20004, "target has exist");
	public static final ResultCode TARGET_RANGE_HAVE_NONE = createResultCode(-20005, "range_have none");
	public static final ResultCode TAIR_INCDECR_OVERFLOW = createResultCode(-20006, "data overflow");
	public static final ResultCode TAIR_DATA_LEN_LIMIT = createResultCode(-20007, "data len too long");

	public static final ResultCode TAIR_IS_NOT_SUPPORT = createResultCode(-20008, "not support");
	public static final ResultCode TAIR_RANGE_OUT_OF_LIMIT = createResultCode(-20010, "range count reach limit");

	public static final ResultCode GC_STATUS_FINISHED = createResultCode(-5201, "gc status unfinished");
	public static final ResultCode GC_STATUS_UNFINISHED = createResultCode(-5202, "gc status finished");
	public static final ResultCode GC_STATUS_FAILED = createResultCode(-5203, "request failed");
	public static final ResultCode NS_READ_ONLY = createResultCode(-5117, "namespace is read only");

	public static final ResultCode TOO_MANY_KEYS = createResultCode(-5118, "too many keys");
	public static final ResultCode WAIT_TOKEN = createResultCode(-5119, "wait token");

	public static final ResultCode TAIR_RPC_OVERFLOW = new ResultCode(-6000, "rpc overflow");

	private int code = 0;
	private String message = null;

	public ResultCode(int code) {
		this.code = code;
	}

	public ResultCode(int code, String message) {
		this.code = code;
		this.message = message;
	}

    private static ResultCode createResultCode(int code, String message) {
        ResultCode resultCode = new ResultCode(code, message);
        if(code == 1) {
            //...
            codeMap.put(-3998, resultCode);
        } else if (code == -10) {
            codeMap.put(-3983, resultCode);
        } else if (code == -20008) {
            codeMap.put(-4001, resultCode);
        }
        codeMap.put(code, resultCode);
        log.debug("create ResultCode {code = " +
                String.valueOf(code) +
                ", message = " + message + "}");
        return resultCode;
    }

	/**
	 * return the ResultCode object of the code.
	 * <p>
	 * If add new ResultCode, remember add case here.
	 */
	public static ResultCode valueOf(int code) {
		ResultCode resultCode = codeMap.get(code);
		if(resultCode == null) {
			return UNKNOW;
		}
		return resultCode;
	}

	/**
	 * @return the inner code of this object
	 */
	public int getCode() {
		return code;
	}

	/**
	 * @return the description of this object
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * whether the request is success.
	 * <p>
	 * if the target is not exist, this method return true.
	 */
	public boolean isSuccess() {
		return code >= 0;
	}

	@Override
	public String toString() {
		return "code=" + code + ", msg=" + message;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj != null && (obj instanceof ResultCode)) {
			ResultCode rc = (ResultCode)obj;
			return rc.getCode() == code;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return code;
	}
}
