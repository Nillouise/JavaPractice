package com.taobao.tair.extend.packet.set.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.packet.BasePacket;

public class RequestSAddMultiPacket extends BasePacket {
	private final static int HEADER_LEN = 1 + 2 + 2 + 4 + 4;
	
	private short namespace = 0;
	private int expire = 0;
	private List<byte[]> keys = new ArrayList<byte[]>();
	private List<Set<byte[]>> valueSets = new ArrayList<Set<byte[]>>();
	private Map<? extends Serializable, Set<? extends Serializable> > keys_values;
	
	public RequestSAddMultiPacket(Transcoder transcoder) {
		super(transcoder);
		pcode = TairConstant.TAIR_REQ_SADDMULTI_PACKET;
	}
	
	public RequestSAddMultiPacket() {
		pcode = TairConstant.TAIR_REQ_SADDMULTI_PACKET;
	}

	@SuppressWarnings("unchecked")
	public int encode() {
		int keybyteslength = 0;
		int valuesbyteslength = 0;
		int valuescount = 0;
		
		Set<?> keys_values_set = keys_values.entrySet();
		for(Object object : keys_values_set) {
			Entry<? extends Serializable, Set<? extends Serializable>> key_values_pair =
					(Entry<? extends Serializable, Set<? extends Serializable>>)object;
			Serializable key = key_values_pair.getKey();
			Set<? extends Serializable> values = key_values_pair.getValue();
			
			if (key == null || values == null || values.size() <= 0) {
				return TairConstant.SERIALIZEERROR;
			}
			
			try {
				byte[] keybytes = transcoder.encode(key);
				keybyteslength += keybytes.length;
				if (keybytes.length >= TairConstant.TAIR_KEY_MAX_LENTH) {
					return TairConstant.KEYTOLARGE;
				}
				keys.add(keybytes);
				Set<byte[]> valueSet = new HashSet<byte[]>();
				for(Serializable value : values) {
					byte[] valuebytes = transcoder.encode(value);
					valuesbyteslength += valuebytes.length;
					if (valuebytes.length >= TairConstant.TAIR_VALUE_MAX_LENGTH) {
						return TairConstant.VALUETOLARGE;
					}
					valueSet.add(valuebytes);
					valuescount++;
				}
				valueSets.add(valueSet);
			} catch (Throwable e) {
				return TairConstant.SERIALIZEERROR;
			}
		}
		
		writePacketBegin(HEADER_LEN + keys_values_set.size() * 8 +
				valuescount * 4 + keybyteslength + valuesbyteslength);
		byteBuffer.put((byte)0);
		byteBuffer.putShort(namespace);
		byteBuffer.putInt(expire);
		byteBuffer.putInt(keys_values_set.size());
		for(int i = 0; i < keys.size(); i++) {
			byte[] keybytes = keys.get(i);
			Set<byte[]> valueSet = valueSets.get(i);
			byteBuffer.putInt(keybytes.length);
			byteBuffer.put(keybytes);
			byteBuffer.putInt(valueSet.size());
			for(byte[] value : valueSet) {
				byteBuffer.putInt(value.length);
				byteBuffer.put(value);
			}
		}
		writePacketEnd();
		
		return 0;
	}
	
	public boolean decode() {
		throw new UnsupportedOperationException();
	}
	
	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}
	public short getNamespace() {
		return this.namespace;
	}
	
	public void setExpire(int expire) {
		this.expire = expire;
	}
	public int getExpire() {
		return this.expire;
	}

	public void setKeysValues(Map<Serializable, Set<? extends Serializable> > keys_values) {
		this.keys_values = keys_values;
	}

	public Object getValue() {
		return this.keys_values;
	}
}
