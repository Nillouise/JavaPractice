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

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TranscoderUtil;

public class ResponseOpCmdPacket extends BasePacket {
	private int code;
	private List<String> values;

	public ResponseOpCmdPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_RESP_OP_CMD_PACKET;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public int getResultCode() {
		return code;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}

	public boolean decode() {
		this.code = byteBuffer.getInt();
		int size = byteBuffer.getInt();

		for (int i = 0; i < size; ++i) {
			int length = byteBuffer.getInt();
			if (length > 0) {
				if (values == null) {
					values = new ArrayList<String>();
				}

				byte[] data = new byte[length-1];
				byteBuffer.get(data);
				values.add(TranscoderUtil.decodeString(data, transcoder.getCharset()));
				byteBuffer.get(); // trailing '\0'
			}
		}
		return true;
	}
}
