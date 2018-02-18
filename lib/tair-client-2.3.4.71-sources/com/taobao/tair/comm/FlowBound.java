package com.taobao.tair.comm;

public class FlowBound {
	int lower;
	int upper;
	
	public FlowBound() {
	}
	
	public FlowBound(int lower, int upper) {
		this.lower = lower;
		this.upper = upper;
	}
	
	public int getLower() {
		return lower;
	}
	public void setLower(int lower) {
		this.lower = lower;
	}
	public int getUpper() {
		return upper;
	}
	public void setUpper(int upper) {
		this.upper = upper;
	}
	@Override
	public String toString() {
		return "(" + lower + ", " + upper + ")";
	}
}
