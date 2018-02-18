package com.taobao.tair.extend.packet.set.response;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.BaseEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class ResponseSMembersMultiPacket extends BasePacket {
	private int configVersion = 0;
	private int resultCode = 0;
	List<Entry<Short, Set<Object>>> valueSetList = new ArrayList<Entry<Short, Set<Object>>>();
	
	public ResponseSMembersMultiPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_RESP_SMEMBERSMULTI_PACKET;
	}

	public int encode() {
		throw new UnsupportedOperationException();
	}
	
	public boolean decode() {
		configVersion 	= byteBuffer.getInt();
		resultCode 		= byteBuffer.getInt();
		
		int entry_size = byteBuffer.getInt();
		
		for(int index = 0; index < entry_size; index++) {
			Set<Object> objectSet = new HashSet<Object>();
			short version 	= byteBuffer.getShort();
			
			Object ovalue = null;
			try {
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
					objectSet.add(ovalue);
				}
				Entry<Short, Set<Object>> entry =
						new BaseEntry<Short, Set<Object>>(version, objectSet);
				valueSetList.add(entry);
			} catch (BufferUnderflowException e) {
				resultCode =  ResultCode.SERIALIZEERROR.getCode();
				return false;
			}
		}
		return true;
	}

	public void setConfigVersion(int configVersion) {
		this.configVersion = configVersion;
	}
	public int getConfigVersion() {
		return this.configVersion;
	}
	
	public List<Entry<Short, Set<Object>>> getValueSetList() {
		return valueSetList;
	}
	
	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}
	
	public int getResultCode() {
		return this.resultCode;
	}
}
