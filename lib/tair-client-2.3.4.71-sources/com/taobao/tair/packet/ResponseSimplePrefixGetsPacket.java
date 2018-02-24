/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.MixedKey;
import com.taobao.tair.etc.TairConstant;

public class ResponseSimplePrefixGetsPacket extends BasePacket {
    protected int             configVersion;
    protected int			  resultCode;
    protected List<Result<DataEntry>> resultEntries = new ArrayList<Result<DataEntry>>();

    public ResponseSimplePrefixGetsPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_SIMPLE_GET_PACKET;
    }
    /**
     * encode
     */
    public int encode() {
        throw new UnsupportedOperationException();
    }
    
    private class Slice {
    	public short  len;
    	public byte[] buf;
    }
    
    private Slice readSlice(java.nio.ByteBuffer byteBuffer) {
    	Slice s = new Slice();
    	s.len = byteBuffer.getShort();
    	if (s.len > 0) {
    		s.buf = new byte[s.len];
    		byteBuffer.get(s.buf);
    	}
    	return s;
    }

    public boolean decode() {
        this.configVersion = byteBuffer.getInt();
        this.resultCode    = byteBuffer.getShort();
        short kvcount = byteBuffer.getShort();
        for (int i = 0; i < kvcount; ++i) {
        	int primecode = byteBuffer.getShort();
        	Slice primeKey = readSlice(byteBuffer);
        	Slice primeVal = readSlice(byteBuffer);
        	Object primeKeyObj = transcoder.decode(primeKey.buf, 2, primeKey.len - 2);
        	
        	if (primeVal.len != 0) {
        		// skip two area bytes
        		Object primeValObj = transcoder.decode(primeVal.buf, 2, primeVal.len - 2);
        		DataEntry da = new DataEntry(primeKeyObj, primeValObj, 0, 0);
        		resultEntries.add(new Result<DataEntry>(ResultCode.valueOf(primecode), da));
        	} 
        	
        	short subcount = byteBuffer.getShort();
        	while(subcount-- > 0) {
        		int	  subcode 	= byteBuffer.getShort();
        		Slice subKey 	= readSlice(byteBuffer);
        		Slice subVal	= readSlice(byteBuffer);
        		Object subKeyObj = transcoder.decode(subKey.buf);
        		Object subValObj = subVal.len == 0 ? 
        				null : 
        				transcoder.decode(subVal.buf, 2, subVal.len - 2);
        		DataEntry da = new DataEntry(subKeyObj, subValObj, 0, 0);
        		resultEntries.add(new Result<DataEntry>(ResultCode.valueOf(subcode), da));
        	}
        }
        return true;
    }

    public List<Result<DataEntry>> getResultEntryList() {
        return resultEntries;
    }
  
    public int getConfigVersion() {
        return configVersion;
    }

    /**
     *
     * @param configVersion the configVersion to setSync
     */
    public void setConfigVersion(int configVersion) {
        this.configVersion = configVersion;
    }

    public int getResultCode() {
        return resultCode;
    }

}
