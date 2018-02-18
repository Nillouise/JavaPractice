package com.taobao.tair.packet;

import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.Transcoder;
import com.taobao.tair.etc.TairConstant;

public class RequestMcOpsPacket extends BasePacket {
	private final static int HEADER_LEN = 1 + 2 + 2 + 4 + 2 + 4 + 4 + 4;
	
	private short namespace;
	private short version;
	private int expire;
	private short mc_opcode;
	private Object key;
	private Object value;
	private byte[] padding;  // pad_len
	
	protected int code;
	
	public RequestMcOpsPacket() {
		super();
		init();
	}
	
	public RequestMcOpsPacket(Transcoder transcoder) {
		super(transcoder);
		init();
	}
	
	private void init() {
		pcode = TairConstant.TAIR_REQ_MC_OPS_PACKET;
		namespace = 0;
		version = 0;
		expire = 0;
		mc_opcode = 0;
		key = null;
		value = null;
		padding = null;
		//use for response mc ops
		code = 0;
	}

	/* (non-Javadoc)
	 * @see com.taobao.tair.packet.BasePacket#encode()
	 */
	@Override
	public int encode() {
		byte[] keybytes = null;
		byte[] valbytes = null;
		int content_len = 0;
		
		try {
			if (key != null) {
				keybytes = transcoder.encode(key);
			}
			if (value != null) {
				valbytes = transcoder.encode(value);
			}
		} catch (Throwable e) {
			return TairConstant.SERIALIZEERROR;
		}
		
		if (keybytes != null) {
			if (keybytes.length > TairConstant.TAIR_KEY_MAX_LENTH) {
				return TairConstant.KEYTOLARGE;
			}
			content_len += keybytes.length;
		}
		
		if (valbytes != null) {
			if (valbytes.length > TairConstant.TAIR_VALUE_MAX_LENGTH) {
				return TairConstant.VALUETOLARGE;
			}
			content_len += valbytes.length;
		}
		
		if (padding != null) {
			content_len += padding.length;
		}
		
		writePacketBegin(HEADER_LEN + content_len);
		byteBuffer.put((byte)0);
		byteBuffer.putShort(namespace);
		byteBuffer.putShort(version);
		byteBuffer.putInt(expire);
		byteBuffer.putShort(mc_opcode);
		if (keybytes != null) {
			byteBuffer.putInt(keybytes.length);
			byteBuffer.put(keybytes);
		} else {
			byteBuffer.putInt(0);
		}
		if (valbytes != null) {
			byteBuffer.putInt(valbytes.length);
			byteBuffer.put(valbytes);
		} else {
			byteBuffer.putInt(0);
		}
		if (padding != null) {
			byteBuffer.putInt(padding.length);
			byteBuffer.put(padding);
		} else {
			byteBuffer.putInt(0);
		}
		writePacketEnd();
		
		return 0;
	}

	/* (non-Javadoc)
	 * @see com.taobao.tair.packet.BasePacket#decode()
	 */
	@Override
	public boolean decode() {
		byteBuffer.get();  // server_flag

		this.namespace = byteBuffer.getShort();
		this.version = byteBuffer.getShort();
		this.expire = byteBuffer.getInt();
		this.mc_opcode = byteBuffer.getShort();

		int keylen = 0;
		int vallen = 0;
		int paddinglen = 0;

		keylen = byteBuffer.getInt();
		if (keylen > 0) {
			try {
				key = transcoder.decode(byteBuffer.array(),
						byteBuffer.position(), keylen);
			} catch (Throwable e) {
				code = ResultCode.SERIALIZEERROR.getCode();
				return false;
			}
			byteBuffer.position(byteBuffer.position() + keylen);
		}
		vallen = byteBuffer.getInt();
		if (vallen > 0) {
			try {
				value = transcoder.decode(byteBuffer.array(),
						byteBuffer.position(), vallen);
			} catch (Throwable e) {
				code = ResultCode.SERIALIZEERROR.getCode();
				return false;
			}
			byteBuffer.position(byteBuffer.position() + vallen);
		}
		paddinglen = byteBuffer.getInt();
		if (paddinglen > 0) {
			try {
				byteBuffer.get(padding, 0, paddinglen);
			} catch (Throwable e) {
				code = ResultCode.SERIALIZEERROR.getCode();
				return false;
			}
		}

		return true;
	}

	public short getNamespace() {
		return namespace;
	}

	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}

	public short getVersion() {
		return version;
	}

	public void setVersion(short version) {
		this.version = version;
	}

	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	public short getMcOpcode() {
		return mc_opcode;
	}

	public void setMcOpcode(short mc_opcode) {
		this.mc_opcode = mc_opcode;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public byte[] getPadding() {
		return padding;
	}

	public void setPadding(byte[] padding) {
		this.padding = padding;
	}
}
