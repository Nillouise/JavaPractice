package com.taobao.tair.comm;

import com.taobao.tair.comm.FlowLimit.FlowStatus;

public class FlowRate {
	long in;
	FlowStatus inStatus;
	
	long out;
	FlowStatus outStatus;
	
	long ops;
	FlowStatus opsStatus;
	
	FlowStatus status;

	public long getIn() {
		return in;
	}

	public void setIn(long in) {
		this.in = in;
	}

	public FlowStatus getInStatus() {
		return inStatus;
	}

	public void setInStatus(FlowStatus inStatus) {
		this.inStatus = inStatus;
	}

	public long getOut() {
		return out;
	}

	public void setOut(long out) {
		this.out = out;
	}

	public FlowStatus getOutStatus() {
		return outStatus;
	}

	public void setOutStatus(FlowStatus outStatus) {
		this.outStatus = outStatus;
	}

	public long getOps() {
		return ops;
	}

	public void setOps(long ops) {
		this.ops = ops;
	}

	public FlowStatus getOpsStatus() {
		return opsStatus;
	}

	public void setOpsStatus(FlowStatus opsStatus) {
		this.opsStatus = opsStatus;
	}

	public FlowStatus getSummaryStatus() {
		return status;
	}

	public void setSummaryStatus(FlowStatus summaryStatus) {
		this.status = summaryStatus;
	}

	@Override
	public String toString() {
		return "in: " + in + " " + inStatus.name() + "; " + 
			   "out: " + out + " " + outStatus.name() + "; " + 
			   "ops: " + ops + " " + opsStatus.name() + "; " +
			   "status: " + status;
	}
	
}
