package com.taobao.tair.packet.stat;

import com.taobao.tair.comm.FlowLimit.FlowStatus;
import com.taobao.tair.comm.FlowRate;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class FlowViewResponse extends BasePacket {
	
	FlowRate rate = new FlowRate();
	
	public FlowViewResponse(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_RESP_FLOW_VIEW;
	}

	private FlowStatus getStatus() {
		int s = byteBuffer.getInt();
		switch (s) {
		case 0:
			return FlowStatus.DOWN;
		case 1:
			return FlowStatus.KEEP;
		case 2:
			return FlowStatus.UP;
		default:
			return FlowStatus.UNKNOW;
		}
	}
	
	public FlowRate getFlowrate() {
		return rate;
	}
	
	public boolean decode() {
		rate.setIn(byteBuffer.getInt());
		rate.setInStatus(getStatus());
		
		rate.setOut(byteBuffer.getInt());
		rate.setOutStatus(getStatus());
		
		rate.setOps(byteBuffer.getInt());
		rate.setOpsStatus(getStatus());
		
		rate.setSummaryStatus(getStatus());
		return true;
	}
}
