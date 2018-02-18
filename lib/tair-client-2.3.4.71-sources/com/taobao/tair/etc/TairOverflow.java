package com.taobao.tair.etc;

public class TairOverflow extends TairClientException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1914156548673342551L;


	public TairOverflow(String message) {
		super(message);
	}

	
	public TairOverflow(String message, Exception e) {
		super(message, e);
	}

}
