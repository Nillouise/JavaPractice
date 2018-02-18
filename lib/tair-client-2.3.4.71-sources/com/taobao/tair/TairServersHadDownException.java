/**  
 * @author yexiang
 * @email yexiang.ych@taobao.com
 * @since 2012-01-05
 */

package com.taobao.tair;

import java.util.concurrent.atomic.AtomicLong;

public class TairServersHadDownException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2403084463823291846L;

	private static AtomicLong occur = new AtomicLong(0);

	public TairServersHadDownException() {
		super();
		occur.incrementAndGet();
	}

	public TairServersHadDownException(String message, Throwable cause) {
		super(message, cause);
		occur.incrementAndGet();
	}

	public TairServersHadDownException(String message) {
		super(message);
		occur.incrementAndGet();
	}

	public TairServersHadDownException(Throwable cause) {
		super(cause);
		occur.incrementAndGet();
	}

	public static long getOccurCount() {
		return occur.get();
	}

	public static void clearOccurCount() {
		occur.set(0);
	}

}
