/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.List;
import java.util.ArrayList;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TranscoderUtil;
import com.taobao.tair.etc.TairConstant;

public class RequestOpCmdPacket extends BasePacket {
	private int cmdType;
	private List<String> cmdParams;

	public RequestOpCmdPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_REQ_OP_CMD_PACKET;
	}
	public void setCmdType(int cmdType) {
		this.cmdType = cmdType;
	}

	public int getCmdType() {
		return cmdType;
	}

	public void setCmdParams(List<String> cmdParams) {
		this.cmdParams = cmdParams;
	}

	public List<String> getCmdParams() {
		return cmdParams;
	}

	public int encode() {
		int length = 0;
		int paramSize = 0;
		List<byte[]> paramData = null;
		boolean hasParam = cmdParams != null && !cmdParams.isEmpty();
		if (hasParam) {
			paramSize = cmdParams.size();
			paramData = new ArrayList<byte[]>(cmdParams.size());
			for (String str : cmdParams) {
				byte[] data = TranscoderUtil.encodeString(str, transcoder.getCharset());
				length += TairConstant.INT_SIZE + data.length + 1 ; // with trailing '\0'
				paramData.add(data);
			}
		}

		writePacketBegin(TairConstant.INT_SIZE * 2 + length);
		byteBuffer.putInt(cmdType);
		byteBuffer.putInt(paramSize);
		if (hasParam) {
			for (byte[] data : paramData) {
				byteBuffer.putInt(data.length + 1);
				byteBuffer.put(data);
				byteBuffer.put((byte)0);
			}
		}
		writePacketEnd();
		return 0;
	}
}