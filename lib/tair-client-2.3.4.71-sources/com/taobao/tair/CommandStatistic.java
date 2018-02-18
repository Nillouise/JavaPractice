package com.taobao.tair;

public interface CommandStatistic {
	void addCommandStat(StatisticInfo si);
	int getNamespace();
	String getUsername();
}
