package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponseQueryBulkWriteTokenPacket extends BasePacket {
    private int    tokenId = 0;
    private int    code = 0;

    public ResponseQueryBulkWriteTokenPacket(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TairConstant.TAIR_RESP_QUERY_BULK_WRITE_TOKEN_PACKET;
    }

    public int encode() {
        int capacity = 8;
        writePacketBegin(capacity);
        byteBuffer.putInt(this.code);
        byteBuffer.putInt(this.tokenId);
        writePacketEnd();

        return 0;
    }

    public boolean decode() {
        this.code = byteBuffer.getInt();
        this.tokenId = byteBuffer.getInt();
        return true;
    }

    public int getCode() {
        return code;
    }

    public int getTokenId() {
    	return this.tokenId;
    }
}
