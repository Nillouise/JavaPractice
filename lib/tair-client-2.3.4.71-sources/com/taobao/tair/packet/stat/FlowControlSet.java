package com.taobao.tair.packet.stat;

import com.taobao.tair.comm.FlowBound;
import com.taobao.tair.comm.FlowLimit.FlowType;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class FlowControlSet extends BasePacket {
	FlowType type;
	FlowBound bound = new FlowBound();
	int namespace;
	int success;
	
	public FlowControlSet(Transcoder transcoder) {
		super(transcoder);
		this.pcode = TairConstant.TAIR_FLOW_CONTROL_SET;
	}
	
	public FlowType getType() {
		return type;
	}

	public void setType(FlowType type) {
		this.type = type;
	}

	public FlowBound getBound() {
		return bound;
	}

	public void setBound(FlowBound bound) {
		this.bound = bound;
	}

	public int getNamespace() {
		return namespace;
	}

	public void setNamespace(int namespace) {
		this.namespace = namespace;
	}

	public boolean isSuccess() {
		return success != 0;
	}

	public int encode() {
        writePacketBegin(0);
        switch (type) {
        case IN:
        	byteBuffer.putInt(0);
        	break;
        case OUT:
        	byteBuffer.putInt(1);
        	break;
        case OPS:
        	byteBuffer.putInt(2);
        	break;
        }
     	byteBuffer.putInt(bound.getLower());
     	byteBuffer.putInt(bound.getUpper());
		byteBuffer.putInt(namespace);
		byteBuffer.putInt(success);
        writePacketEnd();
		return 0;
	}
	
	public boolean decode() {
        int s = byteBuffer.getInt();
        switch (s) {
        case 0:
        	type = FlowType.IN;
        	break;
        case 1:
        	type = FlowType.OUT;
        	break;
        case 2:
        	type = FlowType.OPS;
        	break;
        default:
        	type = FlowType.UNKNOW;
        	break;
        }
        bound.setLower(byteBuffer.getInt());
        bound.setUpper(byteBuffer.getInt());
        namespace = byteBuffer.getInt();
        success = byteBuffer.getInt();
        return true;
    }
}
