package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponseMcOpsPacket extends RequestMcOpsPacket {
	private int configVersion;
	private int code;
	
	public ResponseMcOpsPacket() {
		super();
		init();
	}
	
	public ResponseMcOpsPacket(Transcoder transcoder) {
		super(transcoder);
		init();
	}
	
	private void init() {
		pcode = TairConstant.TAIR_RESP_MC_OPS_PACKET;
		configVersion = 0;
		code = 0;
	}

	/* (non-Javadoc)
	 * @see com.taobao.tair.packet.BasePacket#encode()
	 */
	@Override
	public int encode() {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see com.taobao.tair.packet.BasePacket#decode()
	 */
	@Override
	public boolean decode() {
		try {
			super.decode();
			configVersion = byteBuffer.getInt();
			code = byteBuffer.getShort();
		} catch (Throwable e) {
			return false;
		}
		
		return true;
	}

	/**
	 * @return the configVersion
	 */
	public int getConfigVersion() {
		return configVersion;
	}

	/**
	 * @return the code
	 */
	public int getCode() {
		return code;
	}
}
