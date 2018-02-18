package com.taobao.tair.extend.packet.zet.response;

import java.nio.BufferUnderflowException;

import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.extend.ValueScorePair;
import com.taobao.tair.helper.BytesHelper;

public class ResponseZRevrangeWithScorePacket extends ResponseZRevrangePacket {
	public ResponseZRevrangeWithScorePacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_RESP_ZREVRANGEWITHSCORE_PACKET;
	}

	public boolean decode() {
		Object ovalue = null;
    	try {
	    	configVersion 	= byteBuffer.getInt();
	    	version 		= byteBuffer.getShort();
	    	resultCode 		= byteBuffer.getInt();
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
	        	
	        	double tmp = BytesHelper.LongToDouble_With_Little_Endian(byteBuffer.getLong());
	        	list.add(new ValueScorePair(ovalue, tmp));
	        }
    	} catch (BufferUnderflowException e) {
    		resultCode =  ResultCode.SERIALIZEERROR.getCode();
			return false;
    	}
		
		return true;
	}
}
