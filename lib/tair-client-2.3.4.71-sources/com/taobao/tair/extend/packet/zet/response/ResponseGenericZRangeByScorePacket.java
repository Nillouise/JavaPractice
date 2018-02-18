package com.taobao.tair.extend.packet.zet.response;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.extend.ValueScorePair;
import com.taobao.tair.helper.BytesHelper;
import com.taobao.tair.packet.BasePacket;

public class ResponseGenericZRangeByScorePacket extends BasePacket {

	private int configVersion = 0;
	private short version = 0;
	private int resultCode = 0;
	private List<Object> list = new ArrayList<Object>();
	
	public ResponseGenericZRangeByScorePacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_RESP_GENERIC_ZRANGEBYSCORE_PACKET;
	}

	public int encode() {
		throw new UnsupportedOperationException();
	}
	
	public boolean decode() {
		Object ovalue = null;
    	try {
	    	configVersion 	= byteBuffer.getInt();
	    	version 		= byteBuffer.getShort();
	    	resultCode 		= byteBuffer.getInt();
	    	int scorecount  = byteBuffer.getInt();
	        int count 		= byteBuffer.getInt();
	        for (int i = 0; i < count; ++i) {
	        	ovalue = null;
	        	int valuesize = byteBuffer.getInt();
	        	if (valuesize > 0) {
	        		try {
						ovalue = transcoder.decode(byteBuffer.array(), 
							byteBuffer.position(), valuesize);
					} catch (Throwable e) {
						resultCode =  ResultCode.SERIALIZEERROR.getCode();
						return false;
					}
					if (ovalue == null) {
						resultCode = ResultCode.SERIALIZEERROR.getCode();
						return false;
					}
					byteBuffer.position(byteBuffer.position() + valuesize);
	        	}
	        	
	        	double tmp = 0.0;
	        	if (scorecount > 0) {
	        		tmp = BytesHelper.LongToDouble_With_Little_Endian(byteBuffer.getLong());
	        	}
	        	list.add(new ValueScorePair(ovalue, tmp));
	        }
    	} catch (BufferUnderflowException e) {
    		resultCode =  ResultCode.SERIALIZEERROR.getCode();
			return false;
    	}
		
		return true;
	}
	
	public void setConfigVersion(int configVersion) {
		this.configVersion = configVersion;
	}
	public int getConfigVersion() {
		return this.configVersion;
	}
	
	public short getVersion() {
		return version;
	}
	public void setVersion(short version) {
		this.version = version;
	}
	
	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}
	public int getResultCode() {
		return this.resultCode;
	}
	
	public List<Object> getValues() {
		return list;
	}
	public void setData(List<Object> list) {
		if (list == null) {
			return;
		}
		this.list = list;
	}
}