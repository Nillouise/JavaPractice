package com.taobao.tair.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigServerUpdater extends Thread{
	private static final Logger log = LoggerFactory.getLogger(ConfigServerUpdater.class);
	
	private ConfigServer configServer;
	private boolean running = true;
	private Integer version = 0;
	private AtomicInteger checkTimes = new AtomicInteger(0);
	
	public ConfigServerUpdater(ConfigServer cs) {
		this.configServer = cs;
		setDaemon(true);
	}
	
	public void check(int version) {
		if (version == configServer.getConfigVersion())
			return;
		synchronized (checkTimes) {
			this.version = version;
			checkTimes.incrementAndGet();
			checkTimes.notify();
		}
	}
	
	@Override
	public void run() {
		int preTimes = -1;
		while (running) {
			try {
				int version = 0;
				synchronized (checkTimes) {
					while (preTimes == checkTimes.get() && running)
						checkTimes.wait();
					version = this.version;
					preTimes = checkTimes.get();
				}
				if (running) {
					configServer.checkConfigVersion(version);
				}
			} catch (InterruptedException e) {
				log.error("run err", e);
			} catch (Exception e) {
				log.error("run err", e);
			}
		}
	}
	
	public void close() {
		this.running = false;
		synchronized (checkTimes) {
			checkTimes.notify();
		}
	}
}
