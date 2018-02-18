package com.taobao.tair.comm;
import com.taobao.tair.etc.TairTimeoutException;


public class LogGuardee {
	private static int step = 2;
	 
	public LogGuardee(int step) {
		LogGuardee.step = step;
	}
	public  boolean guardException(TairTimeoutException e) {
		if (TairTimeoutException.getOccurCount() % step == 0) {
			TairTimeoutException.clearOccurCount();
			return true;
		}
		else {
			return false;
		}
	}
	public void setStep(int step) {
		LogGuardee.step = step;
	}
	public int getStep() {
		return LogGuardee.step;
	}
}
