/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestCommandCollection {
    private Map<Long, BasePacket> requestCommandMap = new HashMap<Long, BasePacket>();
    private Map<Long, List<BasePacket>> prefixRequestCommandMap = new HashMap<Long, List<BasePacket>>();
    private List<BasePacket>      resultList        = new ArrayList<BasePacket>();

    public RequestCommandCollection() {
    }

    public BasePacket findRequest(long addr) {
        return requestCommandMap.get(addr);
    }

    public void addRequest(long addr, BasePacket packet) {
        requestCommandMap.put(addr, packet);
    }

    public void addPrefixRequest(long addr, BasePacket packet) {
        List<BasePacket> list = prefixRequestCommandMap.get(addr);
        if (list == null) {
            list = new ArrayList<BasePacket>();
            prefixRequestCommandMap.put(addr, list);
        }
        list.add(packet);
    }

    public Map<Long, BasePacket> getRequestCommandMap() {
        return requestCommandMap;
    }

    public Map<Long, List<BasePacket>> getPrefixRequestCommandMap() {
        return prefixRequestCommandMap;
    }

    /**
     *
     * @param requestCommandMap the requestCommandMap to setSync
     */
    public void setRequestCommandMap(Map<Long, BasePacket> requestCommandMap) {
        this.requestCommandMap = requestCommandMap;
    }

    /**
     *
     * @return the resultList
     */
    public List<BasePacket> getResultList() {
        return resultList;
    }

    /**
     *
     * @param resultList the resultList to setSync
     */
    public void setResultList(List<BasePacket> resultList) {
        this.resultList = resultList;
    }
}
