package com.taobao.tair.etc;

/**
 * @author mobing.fql E-mail: mobing.fql@taobao.com
 * @version 2015-7-12 08:17:17
 */
public class TairSendRequestStatus {

	private boolean flowControl = false;
	private boolean sphControl = false;

	public boolean isFlowControl() {
		return flowControl;
	}

	public void setFlowControl(boolean flowControl) {
		this.flowControl = flowControl;
	}

	public boolean isSphControl() {
		return sphControl;
	}

	public void setSphControl(boolean sphControl) {
		this.sphControl = sphControl;
	}

}
