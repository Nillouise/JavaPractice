package com.taobao.tair;

import org.apache.log4j.Logger;

import com.taobao.tair.packet.BasePacket;

public class TairBaseCallback implements TairCallback {
	final static Logger logger = Logger.getLogger(TairBaseCallback.class);

	private TairCallback tairCallback;

	public TairBaseCallback(TairCallback tairCallback) {
		this.tairCallback = tairCallback;
	}

	public void callback(Exception e) {
		logger.error(e.getMessage());
		if (tairCallback != null) {
			tairCallback.callback(e);
		}
	}

	public void callback(BasePacket packet) {
		logger.debug("saddAsync callback packet");
		if (packet == null) {
			logger.error("packet == null");
		}
		
		if (tairCallback != null) {
			tairCallback.callback(packet);
		}
	}
}
