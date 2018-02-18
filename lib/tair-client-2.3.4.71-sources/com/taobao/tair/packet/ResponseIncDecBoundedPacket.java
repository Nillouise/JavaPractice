package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponseIncDecBoundedPacket extends ResponseIncDecPacket {

	public ResponseIncDecBoundedPacket(Transcoder transcoder) {
		super(transcoder);
		 this.pcode = TairConstant.TAIR_RESP_INC_DEC_BOUNDED_PACKET;
	}

}
