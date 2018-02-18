package com.taobao.tair;

import com.taobao.tair.packet.BasePacket;

public interface TairCallback {
	public void callback(BasePacket packet);

	public void callback(Exception e);
}
