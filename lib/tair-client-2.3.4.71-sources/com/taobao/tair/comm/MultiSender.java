/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.mina.common.IoSession;

import com.taobao.eagleeye.EagleEye;
import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.etc.TairClientException;
import com.taobao.tair.etc.TairSendRequestStatus;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.PacketStreamer;
import com.taobao.tair.packet.RequestCommandCollection;


public class MultiSender {

	private TairClientFactory clientFactory;
    private PacketStreamer   packetStreamer = null;

    public MultiSender(TairClientFactory factory, PacketStreamer packetStreamer) {
        this.packetStreamer = packetStreamer;
        this.clientFactory = factory;
    }

	private void annotateEagleEyeMultiRecv(boolean success, RequestCommandCollection rcList, 
			String pcode, int namespace, String groupName, int requestSize) {
		// EagleEye buried code
		if (pcode != null) {
			EagleEye.requestSize(requestSize);
			List<BasePacket> resultList = rcList.getResultList();
			for (int i = 0; i < resultList.size(); i++) {
				BasePacket result = resultList.get(i);
				EagleEye.startRpc(pcode, groupName);
				EagleEye.responseSize(result.getLen());
				EagleEye.remoteIp(result.getRemoteAddress().toString());
				int rc = result.getResultCode();
				EagleEye.rpcClientRecv(String.valueOf(rc), EagleEye.TYPE_TAIR, String.valueOf(namespace));
			}
			if (success) {
				EagleEye.rpcClientRecv(String.valueOf(ResultCode.SUCCESS.getCode()), 
						EagleEye.TYPE_TAIR, String.valueOf(namespace));
			} else {
				EagleEye.rpcClientRecv(String.valueOf(ResultCode.CONNERROR.getCode()), 
						EagleEye.TYPE_TAIR, String.valueOf(namespace));
			}
		}
	}

    public boolean sendRequest(int namespace, RequestCommandCollection rcList,
			int timeout, int waitTimeout, String groupName,
			TairSendRequestStatus status) {
		if (TairUtil.mockMode) {
			return false;
		}

        Map<Long, BasePacket> map       = rcList.getRequestCommandMap();
        MultiReceiveListener               listener  = new MultiReceiveListener(rcList.getResultList());
        int                                sendCount = 0;

        // EagleEye buried code
        final String pcode;
        if (namespace != 0 && map.size() > 0) {
        	pcode = String.valueOf(map.values().iterator().next().getPcode());
        	EagleEye.startRpc(pcode, groupName);
        	EagleEye.rpcClientSend();
        } else {
        	pcode = null;
        }

        boolean sendAllSuccess = true;
        int requestSize = 0;
        for (Long addr : map.keySet()) {

            TairClient client = null;

            try {
                client = clientFactory.get(TairUtil.idToAddress(addr), timeout, waitTimeout, packetStreamer);
            } catch (TairClientException e) {
            }

            if (client == null) {
                continue;
            }

            BasePacket packet = map.get(addr);
			sendAllSuccess = client.invokeAsync(namespace, packet, timeout,
					listener, SERVER_TYPE.DATA_SERVER, status);
			if (sendAllSuccess == false) {
				break;
			}
            sendCount ++;
            requestSize += packet.getBodyLen();
        }

        listener.await(sendCount, timeout);

        final boolean success = sendAllSuccess && sendCount == listener.doneCount;
		annotateEagleEyeMultiRecv(success, rcList, pcode, namespace, groupName,
				requestSize);
		return success;
    }

	public boolean sendMultiRequest(int namespace,
			RequestCommandCollection rcList, int timeout, int waitTimeout,
			String groupName, TairSendRequestStatus status) {
		if (TairUtil.mockMode) {
			return false;
		}

        Map<Long, List<BasePacket>> map       = rcList.getPrefixRequestCommandMap();
        MultiReceiveListener               listener  = new MultiReceiveListener(rcList.getResultList());
        int                                sendCount = 0;

        // EagleEye buried code
        final String pcode;
        if (namespace != 0 && map.size() > 0) {
        	List<BasePacket> list = map.values().iterator().next();
        	if (list.size() > 0) {
        		pcode = String.valueOf(list.get(0).getPcode());
        		EagleEye.startRpc(pcode, groupName);
        		EagleEye.rpcClientSend();
        	} else {
        		pcode = null;
        	}
        } else {
        	pcode = null;
        }

        boolean sendAllSuccess = true;
        int requestSize = 0;
        for (Entry<Long, List<BasePacket>> entry : map.entrySet()) {

            TairClient client = null;
			long addr = (Long)entry.getKey();

            try {
                client = clientFactory.get(TairUtil.idToAddress(addr), timeout, waitTimeout, packetStreamer);
            } catch (TairClientException e) {
            }

            if (client == null) {
                continue;
            }

            for (BasePacket bp : (List<BasePacket>)entry.getValue()) {
				sendAllSuccess = client.invokeAsync(namespace, bp, timeout,
						listener, SERVER_TYPE.DATA_SERVER, status);
				if (sendAllSuccess == false)
					break;
                sendCount ++;
                requestSize += bp.getBodyLen();
            }
            if (sendAllSuccess == false)
            	break;
        }
        listener.await(sendCount, timeout);
        boolean success = sendAllSuccess && sendCount == listener.doneCount;
        annotateEagleEyeMultiRecv(success, rcList, pcode, namespace, groupName, requestSize);
		return success;
    }

    public class MultiReceiveListener implements ResponseListener {
		private List<BasePacket> resultList = null;
		private ReentrantLock    lock       = null;
		private Condition        cond       = null;
		private int              doneCount  = 0;
		private boolean          isResponseValid = true;

		public MultiReceiveListener(List<BasePacket> resultList) {
			this.resultList = resultList;
			this.lock       = new ReentrantLock();
			this.cond       = this.lock.newCondition();
		}

		public void responseReceived(Object response) {
			lock.lock();

			try {
				if (isResponseValid) {
					resultList.add((BasePacket) response);
					this.doneCount++;
					cond.signal();
				}
			} finally {
				lock.unlock();
			}
		}


		public boolean await(int count, int timeout) {
			long t = TimeUnit.MILLISECONDS.toNanos(timeout);

			lock.lock();
			isResponseValid = true;

			try {
				while (this.doneCount < count) {
					if ((t = cond.awaitNanos(t)) <= 0) {
						return false;
					}
				}
			} catch (InterruptedException e) {
				return false;
			} finally {
				isResponseValid = false;
				lock.unlock();
			}

			return true;
		}

		public void exceptionCaught(IoSession session,
				TairClientException exception) {
		}
    }
}
