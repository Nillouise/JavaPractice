package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponsePrefixIncDecBoundedPacket extends ResponsePrefixIncDecPacket {

	public ResponsePrefixIncDecBoundedPacket(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_RESP_PREFIX_INCDEC_BOUNDED_PACKET;
	}

}
