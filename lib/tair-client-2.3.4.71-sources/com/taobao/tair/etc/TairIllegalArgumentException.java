package com.taobao.tair.etc;

public class TairIllegalArgumentException extends IllegalArgumentException {
	private int errCode;
	private static final long serialVersionUID = 5341764229650795512L;

	public TairIllegalArgumentException(String message, int errCode) {
		super(message);
		this.errCode = errCode;
	}

	public int getErrCode() {
		return errCode;
	}

}
