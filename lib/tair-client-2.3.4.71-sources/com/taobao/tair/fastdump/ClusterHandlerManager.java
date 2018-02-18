/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.fastdump;

import java.util.List;
import java.util.ArrayList;

import java.io.Serializable;

public interface ClusterHandlerManager {
	public boolean canService();

	public boolean update(boolean should);

	public boolean update(List<ClusterInfo> clusterInfos);

	public ClusterHandler pickHandler(Serializable key, int namespace);

	public ClusterHandler[] pickAllHandler();

	public void close();

  public ArrayList<String> getGroupNames();
}
