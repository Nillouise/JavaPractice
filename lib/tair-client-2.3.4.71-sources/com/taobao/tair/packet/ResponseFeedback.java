package com.taobao.tair.packet;

import com.taobao.tair.DataEntry;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class ResponseFeedback extends BasePacket {
	int feedbackType;
	int ns;
	DataEntry key = null;
	public ResponseFeedback(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_RESP_FEEDBACK_PACKET;
	}
	
	public int getFeedbackType() {
	    return feedbackType;
	}
	
	public int getNamespace() {
		return ns;
	}
	
	public DataEntry getKey() {
		return key;
	}
	
	public int encode() {
		throw new UnsupportedOperationException();
	}
	
	public boolean decode() {
		feedbackType = byteBuffer.getInt();
		if (feedbackType == 0) {
			ns = byteBuffer.getShort();
			byte has_key = byteBuffer.get();
			if (has_key > 0) {
				key = new DataEntry();
				removeMetas();
				key.decodeMeta(byteBuffer);
				int msize = byteBuffer.getInt();
				int size = (msize & 0x3FFFFF);
				int prefixSize = (msize >> 22);
				Object obj = null;

				if (size > 0) {
					obj = transcoder.decode(byteBuffer.array(), byteBuffer
							.position(), size, prefixSize);
					byteBuffer.position(byteBuffer.position() + size);
				}
				key.setKey(obj);
			}
		}
		return true;
	}
}
