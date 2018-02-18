package com.taobao.tair.impl;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvalidServer {
    private static final Logger log = LoggerFactory.getLogger(InvalidServer.class);
    private int maxFailCount = 30;
    private String address = null;
    private AtomicInteger failcount = new AtomicInteger(0);
    private boolean useVipServer = false;

    public InvalidServer(String address, boolean useVipServer) {
        this.address = address;
        this.useVipServer = useVipServer;
    }

    public String getAddress() {
        return address;
    }

    public int getFailCount() {
        return failcount.get();
    }

    public void resetFailCount() {
        failcount.set(0);
    }

    public void failed() {
		if (!useVipServer) {
			if (failcount.incrementAndGet() == maxFailCount) {
				log.warn("invalid server " + this.address + " is down");
			}
		}
    }

    public void successed() {
		if (!useVipServer) {
			int now = failcount.addAndGet(-2);
			if (now <= 0)
				failcount.set(0);
		}
    }

    public int getMaxFailCount() {
        return maxFailCount;
    }

    public void setMaxFailCount(int maxFailCount) {
        this.maxFailCount = maxFailCount;
    }

}
