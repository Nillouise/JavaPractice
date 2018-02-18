/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;

import com.taobao.tair.packet.PacketStreamer;


public class TairProtocolCodecFilter extends ProtocolCodecFilter {

	public TairProtocolCodecFilter(PacketStreamer pstreamer) {
		super(new TairProtocolEncoder(), new TairProtocolDecoder(pstreamer));
	}

	@Override
	public void filterWrite(NextFilter nextFilter, IoSession session,
			WriteRequest writeRequest) throws Exception {
		// TODO Auto-generated method stub
		super.filterWrite(nextFilter, session, writeRequest);
	}

}
