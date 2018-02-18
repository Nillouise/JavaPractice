/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mina.common.IoFuture;
import org.apache.mina.common.IoFutureListener;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;

import com.taobao.eagleeye.EagleEye;
import com.taobao.tair.CommandStatistic;
import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.FlowLimit.FlowStatus;
import com.taobao.tair.etc.TairAyncInvokeTimeout;
import com.taobao.tair.etc.TairClientException;
import com.taobao.tair.etc.TairOverflow;
import com.taobao.tair.etc.TairSendRequestStatus;
import com.taobao.tair.etc.TairTimeoutException;
import com.taobao.tair.impl.DefaultTairManager;
import com.taobao.tair.packet.BasePacket;


public class TairClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(TairClient.class);

	private static final boolean isDebugEnabled = LOGGER.isDebugEnabled();

	private static ConcurrentHashMap<Integer, ResponseCallbackTask> configserverTasks =
			new ConcurrentHashMap<Integer, ResponseCallbackTask>();

	private static ConcurrentHashMap<Integer, ResponseCallbackTask> dataserverTasks =
			new ConcurrentHashMap<Integer, ResponseCallbackTask>();

	private int asyncPoolSize = 512;

	private ConcurrentHashMap<Integer, FlowLimit> flowLimitLevel = new ConcurrentHashMap<Integer, FlowLimit>();

	public static enum SERVER_TYPE {
		CONFIG_SERVER, DATA_SERVER, NOCALLBACK
	};

	private static long minTimeout = 100L;

	private static ConcurrentHashMap<Integer, ArrayBlockingQueue<Object>> responses =
			new ConcurrentHashMap<Integer, ArrayBlockingQueue<Object>>();

	private final IoSession session;

	private String key;

	private TairClientFactory clientFactory;
	private DefaultTairManager tairManager = null;
    private static Thread callBackTaskScan = null;
	static {
		Start();
	}

    public static void Destroy() {
		callBackTaskScan.interrupt();
		try {
			callBackTaskScan.join();
		} catch (InterruptedException e) {
			LOGGER.warn("join scan task err", e);
		}
    }
    
    public static void Start() {
    	callBackTaskScan = new Thread(new CallbackTasksScan());
        callBackTaskScan.setName("Thread-" + CallbackTasksScan.class.getName());
        callBackTaskScan.setDaemon(true);
        callBackTaskScan.start();
    }
    
    public void close() {
    	session.close();
    	responses.clear();
    }
	

	protected TairClient(TairClientFactory factory, IoSession session, String key) {
		this.session = session;
		this.key=key;
		this.clientFactory = factory;
	}
	
	public boolean isOverflow(int ns) {
		if (ns <= 0) return false;
		
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null)
			return false;
		boolean ret = flowLimit.isOverflow();
		if (ret) {
			if (isDebugEnabled)
				LOGGER.debug("overflow threshold: " + flowLimit.getThreshold());
		}
		return ret;
	}
	
	public boolean limitLevelUp(int ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			flowLimit = new FlowLimit(ns);
			flowLimitLevel.putIfAbsent(ns, flowLimit);
			flowLimit = flowLimitLevel.get(ns);
		} 
		boolean ret = flowLimit.limitLevelUp();
		LOGGER.warn("overflow threshold up: " + flowLimit.getThreshold());
		return ret;
	}
	
	public boolean limitLevelDown(int ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return false;
		} 
		boolean ret = flowLimit.limitLevelDown();
		LOGGER.warn("oveflow threshold down: " + flowLimit.getThreshold());
		return ret;
	}
	
	public void limitLevelTouch(int ns, FlowStatus status) {
		switch (status) {
		case KEEP:
			limitLevelTouch(ns);
			break;
		case UP:
			limitLevelUp(ns);
			break;
		case DOWN:
			limitLevelDown(ns);
			break;
		default:
			break;
		}
	}
		
	public void limitLevelTouch(int ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return ;
		} 
		flowLimit.limitLevelTouch();
	}
	
	public void checkLevelDown(int ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return ;
		} 
		flowLimit.limitLevelCheck(this);
	}

	private void annotateEagleEyeRecv(int ns, int rc) {
		if (ns != 0) {
			EagleEye.rpcClientRecv(String.valueOf(rc), EagleEye.TYPE_TAIR, String.valueOf(ns));
		}
	}

	private void annotateEagleEyeRecv(int ns, int rc, String username) {
		if (ns != 0) {
			if (null != username)
				EagleEye.rpcClientRecv(String.valueOf(rc), EagleEye.TYPE_TAIR, String.valueOf(ns) + ":" + username);
			else
				EagleEye.rpcClientRecv(String.valueOf(rc), EagleEye.TYPE_TAIR, String.valueOf(ns));
		}
	}

	/**
	 * 
	 * @param ns , access namespace, system call, namespace is 0
	 * @param packet, request packet
	 * @param timeout
	 * @return response packet; if overflow, return null
	 * @throws TairClientException
	 */
	public Object invoke(int ns, final BasePacket packet, final long timeout)
			throws TairClientException {
		return invoke(ns, packet, timeout, null);
	}

	/**
	 *
	 * @param ns , access namespace, system call, namespace is 0
	 * @param packet, request packet
	 * @param timeout
	 * @param groupName
	 * @return response packet; if overflow, return null
	 * @throws TairClientException
	 */
	public Object invoke(int ns, final BasePacket packet, final long timeout, String groupName)
			throws TairClientException {
		if (isDebugEnabled) {
			LOGGER.debug("send request [" + packet.getChid() + "],time is:"
					+ System.currentTimeMillis());
		}

		String username = getUsername(ns);
		TairStatisticInfo tsi = new TairStatisticInfo();
		tsi.setNs(ns);
		tsi.setGroupName(groupName);
		tsi.setPcode(packet.getPcode());
		tsi.setIn(packet.getBodyLen());

		if (isOverflow(ns)) {
			checkLevelDown(ns);
			AddStat(tsi);
			throw new TairOverflow("sync call namespace " + ns + " is over flowing to " + session.getRemoteAddress().toString());
		}


		if (ns != 0) {
			EagleEye.startRpc(String.valueOf(packet.getPcode()), groupName);
		}

		ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(2);
		responses.put(packet.getChid(), queue);

		ByteBuffer bb = packet.getByteBuffer();
		bb.flip();
		byte[] data = new byte[bb.remaining()];
		bb.get(data);

		if (ns != 0) {
			EagleEye.requestSize(packet.getBodyLen());
			EagleEye.remoteIp(session.getRemoteAddress().toString());
			EagleEye.rpcClientSend();
		}

		WriteFuture writeFuture = session.write(data);
		//trace log
		
		writeFuture.addListener(new IoFutureListener() {

			public void operationComplete(IoFuture future) {
			
				WriteFuture wfuture = (WriteFuture) future;
				if (wfuture.isWritten()) {
					packet.hadSent();
					return;
				}
				String error = "send message to tair server error ["
						+ packet.getChid() + "], tair server: "
						+ session.getRemoteAddress()
						+ ", maybe because this connection closed :"
						+ !session.isConnected();
				LOGGER.warn(error);
				TairResponse response = new TairResponse();
				response.setRequestId(packet.getChid());
				response.setResponse(new TairClientException(error));
				try {
					putResponse(packet.getChid(), response.getResponse());
				} catch (TairClientException e) {
					// IGNORE,should not happen
				}
				// close this session
				if(session.isConnected())
					session.close();
				else
					clientFactory.removeClient(key);
			}
			
		});
		Object response = null;
		try {
			response = queue.poll(timeout, TimeUnit.MILLISECONDS);
			if (response != null && response instanceof WaitDecodedResponse) {
				response = queue.poll(500, TimeUnit.MILLISECONDS);
			}
			if (response == null) {
				AddStat(tsi);
				annotateEagleEyeRecv(ns, ResultCode.CONNERROR.getCode(), username);
				throw new TairTimeoutException(
						"tair client invoke timeout, timeout is: " + timeout
								+ ",requestId is: " + packet.getChid() + "request type:" + packet.getClass().getName());
			} else if (response instanceof TairClientException) {
				AddStat(tsi);
				annotateEagleEyeRecv(ns, ResultCode.CONNERROR.getCode(), username);
				throw (TairClientException) response;
			}
		} catch (InterruptedException e) {
			AddStat(tsi);
			annotateEagleEyeRecv(ns, ResultCode.CONNERROR.getCode(), username);
			throw new TairClientException("catch InterruptedException while wait for response ", e);
		} finally {
			responses.remove(packet.getChid());
			// For GC
			queue.clear();
			queue = null;
		}
		if (isDebugEnabled) {
			LOGGER.debug("return response [" + packet.getChid() + "],time is:"
					+ System.currentTimeMillis());
			LOGGER.debug("current responses size: " + responses.size());
		}

		if (response instanceof BasePacket) {
			BasePacket bp = (BasePacket)response;
			// do decode here
			bp.decode();

			tsi.setOut(bp.getLen());
			tsi.setRc(bp.getResultCode());
			AddStat(tsi);
			EagleEye.responseSize(bp.getLen());
			annotateEagleEyeRecv(ns, bp.getResultCode(), username);
		} else {
			AddStat(tsi);
			annotateEagleEyeRecv(ns, ResultCode.CONNERROR.getCode(), username);
		}
		return response;
	}

	public boolean invokeAsync(int namespace, final BasePacket packet,
			long timeout, ResponseListener listener, SERVER_TYPE type, TairSendRequestStatus status) {
		if(isDebugEnabled){
			LOGGER.debug("send request ["+packet.getChid()+"] async,time is:"+System.currentTimeMillis());
		}

		if (isOverflow(namespace)) {
			if (null != status) {
				status.setFlowControl(true);
			}
			checkLevelDown(namespace);
			LOGGER.error("flow limit", new TairOverflow("async call namespace " + namespace + " is over flowing to " + session.getRemoteAddress().toString()));
			return false;
		}

		if(minTimeout>timeout){
			minTimeout=timeout;
		}
		final ResponseCallbackTask callbackTask = new ResponseCallbackTask(
															packet.getChid(),
															listener, 
															this.session, 
															timeout);
		if (type == SERVER_TYPE.CONFIG_SERVER) {
			configserverTasks.put(packet.getChid(), callbackTask);
			
		} else if (type == SERVER_TYPE.DATA_SERVER) {
			if (dataserverTasks.size() >= asyncPoolSize)
				return false;
			else
				dataserverTasks.put(packet.getChid(), callbackTask);
			
		} else if (type == SERVER_TYPE.NOCALLBACK){
			// DO nothing, not regist callback
		} else {
			// won't reach here
			return false;
		}
		ByteBuffer bb = packet.getByteBuffer();
		bb.flip();
		byte[] data = new byte[bb.remaining()];
		bb.get(data);
		
		WriteFuture writeFuture=session.write(data);
		writeFuture.addListener(new IoFutureListener(){

			public void operationComplete(IoFuture future) {
				WriteFuture wfuture=(WriteFuture)future;
				if(wfuture.isWritten()){
					return;
				}
				String error = "send message to tair server error [" + 
								packet.getChid() + "], tair server: " + 
								session.getRemoteAddress() +
								", maybe because this connection closed :" + 
								!session.isConnected();
	            LOGGER.warn(error);
	            callbackTask.setResponse(new TairClientException(error));
				// close this session
				if(session.isConnected())
					session.close();
				else
					clientFactory.removeClient(key);
			}

		});
		return true;
	}

	class WaitDecodedResponse {

	}

	private void AddStat(TairStatisticInfo tsi) {
		if (null != tairManager && 0 != tsi.getNs()) {
			CommandStatistic cstat = tairManager.getCstat(tsi.getNs());
			if (null != cstat) {
				cstat.addCommandStat(tsi);
			}
		}
	}

	private String getUsername(int namespace) {
		if (null != tairManager && 0 != namespace) {
			CommandStatistic cstat = tairManager.getCstat(namespace);
			if (null != cstat) {
				return cstat.getUsername();
			}
		}
		return null;
	}

	protected void onResponseCaught(Integer requestId) {
		ArrayBlockingQueue<Object> queue = responses.get(requestId);
		if (queue != null) {
			queue.offer(new WaitDecodedResponse());
			return;
		}
			
		ResponseCallbackTask task = dataserverTasks.get(requestId);
		if (task != null) {
			task.waitForDecodedResponse();
			return;
		}
	}

	protected void putResponse(Integer requestId, Object response)
			throws TairClientException {
		if (responses.containsKey(requestId)) {
			try {
				ArrayBlockingQueue<Object> queue = responses.get(requestId);
				if (queue != null) {
					queue.put(response);
					if (isDebugEnabled) {
						LOGGER.debug("put response [" + requestId
								+ "], time is:" + System.currentTimeMillis());
					}
				} else if (isDebugEnabled) {
					LOGGER.debug("give up the response,maybe because timeout,requestId is:"
									+ requestId);
				}
				
			} catch (InterruptedException e) {
				throw new TairClientException("put response error", e);
			}
		} else {
			if (isDebugEnabled)
				LOGGER.debug("give up the response,maybe because timeout,requestId is:"
								+ requestId);
		}
	}

	protected boolean putCallbackResponse(Integer requestId, Object response) {
		ResponseCallbackTask task = dataserverTasks.get(requestId);
		if(task == null) {
			task = configserverTasks.get(requestId);
		}  else {
			
		}
		if (task == null) {
			return false;
		}
		task.setResponse(response);
		return true;
	}

	static class CallbackTasksScan implements Runnable{

		static final long DEFAULT_SLEEPTIME = 10L;
		
		boolean isRunning=true;
		
		final TairClientException timeoutException = new TairAyncInvokeTimeout("receive response timeout");
		
		void scanCallbackTasks(ConcurrentHashMap<Integer, ResponseCallbackTask> tasks) {
			List<Integer> removeIds = new ArrayList<Integer>();
			for (Entry<Integer, ResponseCallbackTask> entry : tasks.entrySet()) {
				long currentTime=System.currentTimeMillis();
				ResponseCallbackTask task=entry.getValue();
				if((task.getIsDone().get())){
					removeIds.add(task.getRequestId());
				}
				else if(task.getTimeout() < currentTime){
					removeIds.add(task.getRequestId());
					task.setResponse(timeoutException);
				}
				if (isRunning == false)
					return ;
			}
			for (Integer removeId : removeIds) {
				tasks.remove(removeId);
			}
		}
		
		public void run() {
			while(isRunning){
				scanCallbackTasks(dataserverTasks);
				scanCallbackTasks(configserverTasks);
				if(dataserverTasks.size()==0 && configserverTasks.size() == 0){
					if (isRunning) {
						try {
							Thread.sleep(DEFAULT_SLEEPTIME);
						} 
						catch (InterruptedException e) {
						}
					}
				}
			}
		}
	}
	
	public void setTairManager(DefaultTairManager tm) {
		tairManager = tm;
	}
	
	public DefaultTairManager getTairManager() {
		return tairManager;
	}

	public String toString() {
		if (this.session != null)
			return this.session.toString();
		return "null session client";
	}

}
