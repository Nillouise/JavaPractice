package com.taobao.tair;

public class StatisticInfo {
	private int pcode;
	private int in;
	private int out;
	private boolean hit;
	private boolean fail;
	public int getPcode() {
		return pcode;
	}
	public void setPcode(int pcode) {
		this.pcode = pcode;
	}
	public int getIn() {
		return in;
	}
	public void setIn(int in) {
		this.in = in;
	}
	public int getOut() {
		return out;
	}
	public void setOut(int out) {
		this.out = out;
	}
	public boolean isHit() {
		return hit;
	}
	public void setHit(boolean hit) {
		this.hit = hit;
	}
	public boolean isFail() {
		return fail;
	}
	public void setFail(boolean fail) {
		this.fail = fail;
	}
}
