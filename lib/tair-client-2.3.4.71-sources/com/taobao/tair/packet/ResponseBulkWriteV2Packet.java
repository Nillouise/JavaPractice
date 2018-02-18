package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponseBulkWriteV2Packet extends ResponseBulkWritePacket {
  private int version;

	public ResponseBulkWriteV2Packet(Transcoder transcoder) {
		 super(transcoder);
		 this.pcode = TairConstant.TAIR_RESP_BULK_WRITE_V2_PACKET;
	}

  public boolean decode() {
      this.version       = byteBuffer.getInt();
      super.decode();
      return true;
  }
}
