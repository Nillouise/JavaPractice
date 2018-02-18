/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;

import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.ResponseFeedback;
import com.taobao.tair.packet.stat.FlowControl;


public class TairClientProcessor extends IoHandlerAdapter{

	private static final Logger LOGGER = LoggerFactory.getLogger(TairClientProcessor.class);
	
	private TairClient client=null;
	
	private TairClientFactory factory=null;
	
	private String key=null;
	
	public void setClient(TairClient client){
		this.client=client;
	}
	
	public void setFactory(TairClientFactory factory,String targetUrl){
		this.factory=factory;
		key=targetUrl;
	}
	
	public void responseCaught(int chid) {
		client.onResponseCaught(chid);
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("response get " + chid);
	}
	
	@Override
	public void messageReceived(IoSession session, Object message)
			throws Exception {
		TairResponse response=(TairResponse)message;
		Integer requestId=response.getRequestId();
		
	    if(response.getResponse() instanceof BasePacket){
			/**
			 * åŠ å…¥ è¿�ç¨‹æœ�åŠ¡ åœ°å�€ä¿¡æ�¯
			 * @author xiaodu
			 */
			BasePacket tmp = (BasePacket)response.getResponse();
			tmp.setRemoteAddress(session.getRemoteAddress());
		}
	    if (requestId == -1) {
	    	if (response.getResponse() instanceof FlowControl) {
	    		FlowControl flow = (FlowControl) response.getResponse();
	    		flow.decode();
	    		client.limitLevelTouch(flow.getNamespace(), flow.getStatus());
	    	} else if (response.getResponse() instanceof ResponseFeedback) {
	    		ResponseFeedback fdbk = (ResponseFeedback) response.getResponse();
	    		fdbk.decode();
	    		if (client != null) {
	    			client.getTairManager().enhanceLocalCache(fdbk.getNamespace());
	    			LOGGER.warn("enhance local cache of namespace " + fdbk.getNamespace());
	    		}
	    		LOGGER.info("Received Feedback Response, ns: " + fdbk.getNamespace() + " key: " + fdbk.getKey());
	    	} else {
	    		LOGGER.error("get FlowControl packet, but cast failed, Response " + response.getResponse().getClass());
	    	}
			
		} else {
			if (client == null) {
				LOGGER.error("receive messag, but callback null: " + message);
			}
			if (!client.putCallbackResponse(requestId, response.getResponse())) {
				client.putResponse(requestId, response.getResponse());
			}
		}
	}
	
	@Override
	public void exceptionCaught(IoSession session, Throwable cause)
			throws Exception {
		if (LOGGER.isWarnEnabled())
			LOGGER.warn("connection exception occured", cause);
		
		if(!(cause instanceof IOException)){
			session.close();
		}
	}

	public void sessionClosed(IoSession session) throws Exception {
		factory.removeClient(key);
	}

	private String getRemoteId(InetSocketAddress remoteAddr) {
		return remoteAddr.getAddress().getHostAddress() + ":"
				+ remoteAddr.getPort();
	}

	public void sessionOpened(IoSession session) throws Exception {
		LOGGER.info("open session : "
				+ getRemoteId((InetSocketAddress) session.getRemoteAddress()));
	}

	public void sessionIdle(IoSession session, IdleStatus status)
			throws Exception {
		LOGGER.info("close session : "
				+ getRemoteId((InetSocketAddress) session.getRemoteAddress()));
		session.close();
	}
}
