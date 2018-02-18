/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mina.common.IoSession;

import com.taobao.tair.TairServersHadDownException;
import com.taobao.tair.comm.ResponseListener;
import com.taobao.tair.comm.TairClient;
import com.taobao.tair.comm.TairClient.SERVER_TYPE;
import com.taobao.tair.comm.TairClientFactory;
import com.taobao.tair.etc.TairClientException;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.etc.TairUtil;
import com.taobao.tair.packet.BasePacket;
import com.taobao.tair.packet.PacketStreamer;
import com.taobao.tair.packet.RequestGetGroupPacket;
import com.taobao.tair.packet.RequestQueryInfoPacket;
import com.taobao.tair.packet.ResponseGetGroupPacket;
import com.taobao.tair.packet.ResponseQueryInfoPacket;

public class ConfigServer implements ResponseListener {
	
	
	
  private static final Logger log = LoggerFactory.getLogger(ConfigServer.class);
  private static final int MURMURHASH_M = 0x5bd1e995;
  private String groupName = null;
  private int configVersion = 0;
  private AtomicLong retrieveLastTime = new AtomicLong(0);

  private List<String> configServerList = new ArrayList<String>();

  private List<Long> serverList;

  private boolean forceService = false;

  private PacketStreamer pstream;

  private int bucketCount = 0;
  private int copyCount = 0;

  private Set<Long> aliveNodes;
  private Set<Long> downNodes;

  private int masterFailCount;

  private final int MAX_MASTER_FAILCOUNT = 3;

  private TairClientFactory factory;
  
  private InvalidServerManager invalidServer;
  
  private AdminServerManager adminServer;
  
  private boolean checkDownNodes = false;
  
	  private boolean supportBackupMode = false;
	  
	  private int refluxRatio = 0;
	  
	  protected Map<Long, String> happendDownNodes = new ConcurrentHashMap<Long, String>();
  
  private final int CS_HEALTH_DEFUALT = 3;
  private AtomicInteger csHealth = new AtomicInteger(CS_HEALTH_DEFUALT);

  public ConfigServer(TairClientFactory factory, String groupName, List<String> configServerList,
      PacketStreamer pstream, InvalidServerManager invalidServerManager,
      AdminServerManager adminServerManager) {
    this.groupName = groupName;
    this.pstream = pstream;
    this.masterFailCount = 0;
    this.factory = factory;
    this.invalidServer = invalidServerManager; 
    this.adminServer = adminServerManager; 
    for (String host : configServerList)
      this.configServerList.add(host.trim());
  }
  public ConfigServer(TairClientFactory factory, String groupName, List<String> configServerList,
      PacketStreamer pstream, InvalidServerManager invalidServerManager)
  {
     this(factory, groupName, configServerList, pstream, invalidServerManager, null);
  }
  
	public int getRefluxRatio() {
		return refluxRatio;
	}

	public void setRefluxRatio(int refluxRatio) {
		this.refluxRatio = refluxRatio;
	}

	public void setSupportBackupMode(boolean flag) {
		this.supportBackupMode = flag;
	}
  
	public void setCheckDownNodes(boolean check) {
		this.checkDownNodes = check;
	}

	protected void resetConfigVersion() {
		this.configVersion = 1;
	}
  
	public int getVersion() {
		return this.configVersion;
	}
  
	private int findServerIdx(long hash) {
		log.debug("hashcode: " + hash + ", bucket count: " + bucketCount
				+ " bucket: " + (hash % bucketCount));
		if ((serverList != null) && (serverList.size() > 0))
			return (int) (hash %= bucketCount);
		return -1;
	}
	
	protected int findServerIdx(byte[] keyByte) {
	    long hash = murMurHash(keyByte); // cast to int is safe
	    return findServerIdx(hash);
	}

  public int getBucketCount() {
	return bucketCount;
  }

  public int getCopyCount() {
	 return copyCount;
  }

  public Set<Long> getAliveNodes() {
	  return aliveNodes;
  }

  public int getConfigVersion() {
	  return configVersion;
  }

  public void setConfigVersion(int configVersion) {
	  this.configVersion = configVersion;
  }

  public void setForceService(boolean force) {
      this.forceService = force;
  }

  public List<Long> getServerList() {
      return serverList;
  }
	  
  	public void reset() {
  		happendDownNodes.clear();
		checkConfigVersion(0);
	}
  	
  	public Map<Long, String> getHappendDownNodes() {
  		return happendDownNodes;
  	}

	public long getServer(int serverIdx, boolean isRead) {
		if (serverIdx < 0)
			return 0;
		long serverIp = 0;
		serverIp = serverList.get(serverIdx);
		if (log.isDebugEnabled()) {
			log.debug("oroginal target server: "
					+ TairUtil.idToAddress(serverIp) + " alive server: "
					+ aliveNodes);
		}

		if (!aliveNodes.contains(serverIp)) {
			if (log.isDebugEnabled()) {
				log.debug("master server " + TairUtil.idToAddress(serverIp)
						+ " had down" + copyCount);
			}
			serverIp = 0;
		}

		if (serverIp != 0 && checkDownNodes && downNodes.contains(serverIp)) {
			if (log.isDebugEnabled()) {
				log.debug("master server " + TairUtil.idToAddress(serverIp)
						+ " up, but not service");
			}
			serverIp = 0;
		}

		if (serverIp == 0 && isRead) {
			for (int i = 1; i < copyCount; ++i) {
				int copyServerIdx = serverIdx + i * bucketCount;
				serverIp = serverList.get(copyServerIdx);
				if (log.isDebugEnabled()) {
					log.debug("read operation try: "
							+ TairUtil.idToAddress(serverIp));
				}
				if (aliveNodes.contains(serverIp)
						&& (!checkDownNodes || !downNodes.contains(serverIp))) {
					break;
				} else {
					serverIp = 0;
				}
			}
			if (serverIp == 0) {
				if (log.isDebugEnabled()) {
					log.debug("slave servers also" + " had down");
				}
			}
		}
		return serverIp;
	}
  
    public long getServer(byte[] keyByte, boolean isRead) {
  		if (serverList == null || serverList.size() == 0) {
			log.error("server list is empty");
			return 0;
		}
  		long 	hash 		= murMurHash(keyByte);
		int 	serverIdx 	= findServerIdx(hash);
		long 	serverIp 	= getServer(serverIdx, isRead);
		
  		if (supportBackupMode) {
  			if (serverIp == 0) {
  				serverIp = serverList.get(serverIdx);
  				happendDownNodes.put(serverIp, TairUtil.idToAddress(serverIp));
  				throw new TairServersHadDownException("find server down " + TairUtil.idToAddress(serverIp));
  			} else if (happendDownNodes.containsKey(serverIp) && refluxRatio < 100 && Math.abs(hash % 100) < (100 - refluxRatio)) {
  				throw new TairServersHadDownException("server has down " + TairUtil.idToAddress(serverIp));
  			}
  		}
  		return serverIp;
	}

	public int getBucket(byte[] keyByte) {
		return findServerIdx(keyByte);
	}

  public Map<String, String> grabGroupConfigMap() {
	  RequestGetGroupPacket request = new RequestGetGroupPacket(null);
	  request.setGroupName(groupName);
	  // force get group info, if client version equals server version, cs return empty
	  request.setConfigVersion(0);

	  for (String addr  : configServerList) {
		  log.info("send request to " + addr);
		  ResponseGetGroupPacket response = null;
	      try {
	        TairClient client = factory.get(addr,
	            TairConstant.DEFAULT_CS_CONN_TIMEOUT, TairConstant.DEFAULT_CS_TIMEOUT, pstream);
	        response = (ResponseGetGroupPacket) client.invoke(0, request, TairConstant.DEFAULT_CS_TIMEOUT);
	      } catch (Exception e) {
	        log.error("get config failed from: " + addr, e);
	        continue;
	      }
	      if (response != null) {
			  configVersion = response.getConfigVersion();
	    	  return response.getConfigMap();
	      }
	  }
	  log.error("get config map null");
	  return null;
  }

  public boolean retrieveConfigure() {
    retrieveLastTime.set(System.currentTimeMillis());

    RequestGetGroupPacket packet = new RequestGetGroupPacket(null);

    packet.setGroupName(groupName);
    packet.setConfigVersion(configVersion);


    boolean initSuccess = false;

    for (int i = 0; i < configServerList.size(); i++) {
    	String addr = configServerList.get(i);

		log.info("init configs from configserver: " + addr);

		BasePacket returnPacket = null;
		try {
			TairClient client = factory.get(addr, TairConstant.DEFAULT_CS_CONN_TIMEOUT, 
					TairConstant.DEFAULT_CS_TIMEOUT, pstream);
			returnPacket = (BasePacket) client.invoke(0, packet,
					TairConstant.DEFAULT_CS_TIMEOUT);
		} catch (Exception e) {
			log.error("get config failed from: " + addr, e);
			continue;
		}

      if ((returnPacket != null)
          && returnPacket instanceof ResponseGetGroupPacket) {
        ResponseGetGroupPacket r = (ResponseGetGroupPacket) returnPacket;

        configVersion = r.getConfigVersion();
        bucketCount = r.getBucketCount();
        copyCount = r.getCopyCount();
        aliveNodes = r.getAliveNodes();
        downNodes  = r.getDownServers();

        if (!forceService && (aliveNodes == null || aliveNodes.isEmpty())) {
          log.error("fatal error, no datanode is alive, group name: " + this.groupName + ", server" + addr);
          continue;
        }

        if (log.isInfoEnabled()) {
          for (Long id : aliveNodes) {
            log.info("alive datanode: " + TairUtil.idToAddress(id));
          }
        }

        if (bucketCount <= 0 || copyCount <= 0)
        {
          log.error("bucketcount:" + bucketCount + " copyCount"+copyCount + " " + aliveNodes + configVersion);
          throw new IllegalArgumentException("bucket count or copy count can not be 0");
        }

        if (invalidServer != null && !invalidServer.isUseVipServer())
            invalidServer.updateInvalidServers(r.getConfigMap());

        if (adminServer != null)
        	adminServer.updateAdminServer(r.getConfigMap());

        if ((r.getServerList() != null)
            && (r.getServerList().size() > 0)) {
          this.serverList = r.getServerList();
          if (log.isDebugEnabled()) {
            for (int idx = 0; idx < r.getServerList().size(); idx++) {
              log.debug("+++ " + idx + " => "
                  + TairUtil.idToAddress(r.getServerList().get(idx)));
            }
          }
          if ((this.serverList.size() % bucketCount) != 0) {
            log.error("server size % bucket number != 0, server size: "
                + this.serverList.size()
                + ", bucket number"
                + bucketCount
                + ", copyCount: " + copyCount);
          } else {
            log.warn("configuration inited with version: " + configVersion
                + ", bucket count: " + bucketCount + ", copyCount: "
                + copyCount);
            initSuccess = true;
            break;
          }
        } else if (forceService) {
			initSuccess = true;
			break;
		} else {
          log.warn("server list from config server is null or size is 0");
        }

      } else {
        log.error("retrive from config server " + addr
            + " failed, result: " + returnPacket);
      }
    }

    return initSuccess;
  }

  public Map<String, String>retrieveStat(int qtype, String groupName, long serverId) {

    RequestQueryInfoPacket packet = new RequestQueryInfoPacket(null);

    packet.setGroupName(groupName);
    packet.setQtype(qtype);
    packet.setServerId(serverId);
    Map <String, String> statInfo = null;

    for (int i = 0; i < configServerList.size(); i++) {
      String addr = configServerList.get(i);

      BasePacket returnPacket = null;
      try {
        TairClient client = factory.get(addr,
            TairConstant.DEFAULT_CS_CONN_TIMEOUT, TairConstant.DEFAULT_CS_TIMEOUT, pstream);
        returnPacket = (BasePacket) client.invoke(0, packet,
            TairConstant.DEFAULT_CS_TIMEOUT);
      } catch (Exception e) {
        log.error("get stat failed " + addr, e);
        continue;
      }

      if ((returnPacket != null)
          && returnPacket instanceof ResponseQueryInfoPacket) {
        ResponseQueryInfoPacket r = (ResponseQueryInfoPacket) returnPacket;
        statInfo = r.getKv();

        break;
      } else {
        log.error("retrive stat from config server " + addr
            + " failed, result: " + returnPacket);
      }

    }

    return statInfo;
  }

	public void checkConfigVersion(int version) {
		if (version == configVersion) {
			return;
		}

		if (retrieveLastTime.get() > (System.currentTimeMillis() - 5000)) {
			log.debug("last check time is less than 5 seconds, need not sync");
			return;
		}

		retrieveLastTime.set(System.currentTimeMillis());

		RequestGetGroupPacket packet = new RequestGetGroupPacket(null);

		packet.setGroupName(groupName);
		packet.setConfigVersion(configVersion);
		int i = 0;
		for (i = 0; i < configServerList.size(); i++) {
			if (i == 0 && masterFailCount > MAX_MASTER_FAILCOUNT) {
				masterFailCount = 0;
				continue;
			}

			String host = configServerList.get(i);
			try {
				TairClient client = factory.get(host,
						TairConstant.DEFAULT_CS_CONN_TIMEOUT,
						TairConstant.DEFAULT_CS_TIMEOUT, pstream);
				client.invokeAsync(0, packet, TairConstant.DEFAULT_CS_TIMEOUT,
						this, SERVER_TYPE.CONFIG_SERVER, null);
				break;
			} catch (TairClientException e) {
				log.error("get client failed", e);
				continue;
			}
		}
		if (i == configServerList.size()) {
			// all config server dead
			csHealth.decrementAndGet();
			log.error("all config servers dead");
		} else {
			// at least one cs alive
			csHealth.set(CS_HEALTH_DEFUALT);
		}
	}
	
	public boolean isAllDead() {
		return csHealth.get() < 0;
	}

 	public void responseReceived(Object packet) {
 		if (packet == null || !(packet instanceof ResponseGetGroupPacket)) {
 			log.error("tair client bug:" + packet.getClass().getName() + 
 					  " is not(or be null) " + ResponseGetGroupPacket.class.getName());
 			return ;
 		}
	    ResponseGetGroupPacket groupInfo = (ResponseGetGroupPacket) packet;
	    groupInfo.decode();
	    log.warn("GroupInfo received, " + 
	    		 "ConfigServer:" + groupName + " " + System.identityHashCode(this) + 
	    		 ",current version:" + configVersion + 
	    		 ", new verion:" + groupInfo.getConfigVersion());
	    // check version is not 0 or same as current version
	    if (groupInfo.getConfigVersion() == 0) {
	    	log.warn("new version is 0, will not update");
	    	return ;
	    } else if (configVersion == groupInfo.getConfigVersion()) {
	    	log.warn("current and new version is same, will not update");
	    	return ;
	    } 
		// version is need be updated
		configVersion = groupInfo.getConfigVersion();
	    // update aliveNodes, downServers, invalidserver, serverList
	    // aliveNodes 
		
		if (groupInfo.getAliveNodes() == null || groupInfo.getAliveNodes().isEmpty()) {
	    	log.error("alive nodes is empty, no data servers live");
	    } else {
	    	if (this.aliveNodes != null && this.aliveNodes.size() > 0) {
	    		Set<Long> aliveNodesSet = new HashSet<Long>();
	    		aliveNodesSet.addAll(groupInfo.getAliveNodes());
	    		for (Long id : aliveNodes) {
	    			if (aliveNodesSet.remove(id)) {
	    				log.info("keep alive node: " + TairUtil.idToAddress(id));
	    			} else {
	    				log.warn("host down node: " + TairUtil.idToAddress(id));
	    			}
			    }
	    		for (Long id : aliveNodesSet) {
	    			log.info("host up node: " + TairUtil.idToAddress(id));
	    		}
	    	}
	    }
	    this.aliveNodes = groupInfo.getAliveNodes();
	    
	    // downServers, just update
	    downNodes = groupInfo.getDownServers();
	    // invalidServer, never be null !
	    if (!invalidServer.isUseVipServer())
		    invalidServer.updateInvalidServers(groupInfo.getConfigMap());
	    // server list, if server list is empty, will not update
	    if (groupInfo.getServerList() !=  null && groupInfo.getServerList().size() > 0) {
	    	this.serverList = groupInfo.getServerList();
	    	if (log.isDebugEnabled()) {
	    		int idx = 0;
	    		for (Long addr : this.serverList) {
	    			log.debug("+++ " + idx++ + " => " + TairUtil.idToAddress(addr));
	    		}
	         }
	    } else{
	    	log.error("server list is empty, please check ConfigServer !");
	    }
  }

  public void exceptionCaught(IoSession session, TairClientException exception) {
    log.error("do async request failed", exception);
    if (session.isConnected()) {
    	log.error("session closing");
    	session.close();
    }
    masterFailCount++;
  }

  private long murMurHash(byte[] key) {
    int len = key.length;
    int h = 97 ^ len;
    int index = 0;

    while (len >= 4) {
      int k = (key[index] & 0xff) | ((key[index + 1] << 8) & 0xff00)
        | ((key[index + 2] << 16) & 0xff0000)
        | (key[index + 3] << 24);

      k *= MURMURHASH_M;
      k ^= (k >>> 24);
      k *= MURMURHASH_M;
      h *= MURMURHASH_M;
      h ^= k;
      index += 4;
      len -= 4;
    }

    switch (len) {
      case 3:
        h ^= (key[index + 2] << 16);

      case 2:
        h ^= (key[index + 1] << 8);

      case 1:
        h ^= key[index];
        h *= MURMURHASH_M;
    }

    h ^= (h >>> 13);
    h *= MURMURHASH_M;
    h ^= (h >>> 15);
    return ((long) h & 0xffffffffL);
  }
}
