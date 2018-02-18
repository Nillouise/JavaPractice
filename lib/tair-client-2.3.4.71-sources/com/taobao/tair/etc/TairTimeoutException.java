package com.taobao.tair.etc;

import java.util.concurrent.atomic.AtomicLong;

public class TairTimeoutException extends TairClientException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static AtomicLong occur = new AtomicLong(0);
	
	public TairTimeoutException(Throwable cause) {
		super(cause);
		occur.incrementAndGet();
	}

	public TairTimeoutException(String message, Exception e) {
		super(message,e);
		occur.incrementAndGet();
	}

	public TairTimeoutException(String message) {
		super(message);
		occur.incrementAndGet();
	}
	public static long getOccurCount() {
		return occur.get();
	}

	public static void clearOccurCount() {
		occur.set(0);
	}
}
