package com.taobao.tair.comm;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.packet.stat.FlowCheck;

public class FlowLimit {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FlowLimit.class);
	
	private int threshold;
	private long lastTime;
	private long checkTime;

	private static final double UP_FACTOR = 0.3;
	private static final long UP_CHECKTIME = 10 * 1000; // 10s

	private static final long DOWN_CHECKTIME = 5 * 1000; // 5s
	private static final double DOWN_FACTOR = 0.5;

	private static final int MAX_THRESHOLD = 1000;
	private static final Random flowRandom = new Random();
	
	private static final int CHECK_TIMEOUT = 500; // 500ms
	
	private int namespace;
	
	public static enum FlowStatus {
		KEEP , UP, DOWN, UNKNOW
	}
	
	public static enum FlowType {
		IN, OUT, OPS, UNKNOW
	}
	
	public FlowLimit(int ns) {
		threshold = 0;
		lastTime = 0;
		checkTime = 0;
		namespace = ns;
	}
	
	public int getThreshold() {
		return threshold;
	}
	
	public boolean isOverflow() {
		if (threshold == 0)
			return false;
		else if (threshold >= MAX_THRESHOLD) {
			LOGGER.warn("Threshold " + threshold + " larger than max " + MAX_THRESHOLD);
			return true;
		} else
			return flowRandom.nextInt(MAX_THRESHOLD) < threshold;
	}
	
	public boolean limitLevelUp() {
		long now = System.currentTimeMillis();
		if (now - lastTime < UP_CHECKTIME) {
			return false;
		}
		synchronized (this) {
			if (now - lastTime < UP_CHECKTIME) {
				return true;
			}
			if (threshold < MAX_THRESHOLD - 10)
				threshold = (int)(threshold + UP_FACTOR * (MAX_THRESHOLD - threshold));
			lastTime = now;
			LOGGER.warn("flow limit up ns " + namespace + " curt " + threshold);
		}
		return true;
	}
	
	public void limitLevelTouch() {
		lastTime = System.currentTimeMillis();
	}
	
	public boolean limitLevelDown() {
		long now = System.currentTimeMillis();
		if (now - lastTime < DOWN_CHECKTIME) {
			return false;
		}
		synchronized (this) {
			if (now - lastTime < DOWN_CHECKTIME) {
				return true;
			}
			threshold = (int)(threshold - DOWN_FACTOR * threshold);
			if (threshold < 50)
				threshold = 0;
			lastTime = now;
			LOGGER.warn("flow limit down ns " + namespace + " curt " + threshold);
		}
		return true;
	}

	public boolean limitLevelCheck(TairClient client) {
		if (threshold == 0) {
			return false;
		}

		long now = System.currentTimeMillis();
		if (now - lastTime < DOWN_CHECKTIME || now - checkTime < DOWN_CHECKTIME) {
			return false;
		}
		synchronized (this) {	
			if (now - lastTime < DOWN_CHECKTIME || now - checkTime < DOWN_CHECKTIME) {
				return true;
			}
			checkTime = now;
		}
		LOGGER.warn("flow limit check ns " + namespace + " curt " + threshold);
		FlowCheck request = new FlowCheck(new DefaultTranscoder(0, null));
		request.setNamespace(namespace);
		client.invokeAsync(-1, request, CHECK_TIMEOUT, null, SERVER_TYPE.NOCALLBACK, null);
		return true;
	}
}
