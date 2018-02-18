/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestPingPacket extends BasePacket {
	private int configVersion = 0;
	private int value = 0;
	
	public RequestPingPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_PING_PACKET;
	}
	
	public int encode() {
		writePacketBegin(8);
		byteBuffer.putInt(configVersion);
		byteBuffer.putInt(value);
		writePacketEnd();
		return 0;
	}
	
	public boolean decode() {
		throw new UnsupportedOperationException();
	}
	
	public int getConfigVersion() {
		return this.configVersion;
	}
	
	public void setConfigVersion(int configVersion) {
		this.configVersion = configVersion;
	}
	
	public int getValue() {
		return this.value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
}
