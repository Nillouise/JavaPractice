/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.packet;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestPrefixHidesByProxyPacket extends RequestHideByProxyPacket {
    public RequestPrefixHidesByProxyPacket(Transcoder transcoder, String groupname) {
        super(transcoder, groupname);
        this.pcode = TairConstant.TAIR_REQ_PREFIX_HIDES_BY_PROXY_PACKET;
    }
}
