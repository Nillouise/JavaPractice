package com.taobao.tair.packet.stat;

import com.taobao.tair.comm.FlowLimit.FlowStatus;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class FlowControl extends BasePacket {
	int ns;
	FlowStatus status;

	public FlowControl(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_FLOW_CONTROL;
	}

	public int encode() {
		throw new UnsupportedOperationException();
	}


	public boolean decode() {
		int s = byteBuffer.getInt();
		switch (s) {
		case 0:
			status = FlowStatus.DOWN;
			break;
		case 1:
			status = FlowStatus.KEEP;
			break;
		case 2:
			status = FlowStatus.UP;
			break;
		default:
			return false;
		}
		
		ns = byteBuffer.getInt();
		return true;
	}

	public FlowStatus getStatus() {
		return status;
	}
	
	public int getNamespace() {
		return ns;
	}
}
