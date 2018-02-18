package com.taobao.tair.comm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalCache<KeyType, ValueType> {
	private static final Logger log = LoggerFactory.getLogger(LocalCache.class);

	private Class<ValueType> valueClass;

	private int capability = DEFAULT_CAPABILITY;
	private long expireTime = DEFAULT_EXPIRETIME;

	public static final int DEFAULT_CAPABILITY = 30;
	public static final int DEFAULT_EXPIRETIME = 30;
	public static final int MAX_CAPABILITY_DELTA = 50;
	public static final int MAX_EXPIRETIME_DELTA = 500;
	private static final long ENHANCE_INTERVAL = 100; // ms
	private static final long UNDO_ENHANCE_INTERVAL = 15 * 60 * 1000; // 15min

	private LinkedHashMap<KeyType, Element> lruCacheImpl;

	private ClassLoader customClassLoader = null;

	private long lastEnhanceTime = System.currentTimeMillis();
	private int currentCapDelta = 0;
	private int currentExpDelta = 0;
    //whether need to clone value when put and get value from local cache.
    private boolean isNeedClone = true;

	class Element {
		ValueType value;
		long lastUpdateTime;

		public Element(ValueType value) {
			super();
			this.value = value;
			updateLastUpdateTime();
		}

		public ValueType getValue() {
			return value;
		}

		public long getLastUpdateTime() {
			return lastUpdateTime;
		}

		public void updateLastUpdateTime() {
			this.lastUpdateTime = System.currentTimeMillis();
		}
	}

	public LocalCache(String id, Class<ValueType> valueClass, ClassLoader custerClassLoader) {
		this.valueClass = valueClass;
		lruCacheImpl = new LinkedHashMap<KeyType, Element>(capability + 1, 1.0f, true);
		customClassLoader = custerClassLoader;
	}

    public LocalCache(String id, int capability, long expireTimeMS, Class<ValueType> valueClass, ClassLoader custerClassLoader) {
        this.valueClass = valueClass;
        this.capability = capability;
        this.expireTime = expireTimeMS;
        lruCacheImpl = new LinkedHashMap<KeyType, Element>(capability + 1, 1.0f, true);
        customClassLoader = custerClassLoader;
    }

	public void setExpireTime(long expireTimeMS) {
		this.expireTime = expireTimeMS;
	}

	public void setCapacity(int cap) {
		if (cap >= 0)
			this.capability = cap;
		if (cap == 0)
			clear();
	}

    public boolean isNeedClone() {
        return isNeedClone;
    }

    public void setNeedClone(boolean isNeedClone) {
        this.isNeedClone = isNeedClone;
    }

    public long getExpireTime() {
		return expireTime;
	}

	synchronized public int size() {
		return lruCacheImpl.size();
	}

	public void destroy() {
		this.clear();
	}

	public void setCustomClassLoader(ClassLoader customClassLoader) {
		clear();
		this.customClassLoader = customClassLoader;
	}
	synchronized public void clear() {
		lruCacheImpl.clear();
	}

	synchronized public void del(KeyType key) {
		lruCacheImpl.remove(key);
	}

	public void put(KeyType key, ValueType value) {
		tryUndoEnhance();
        if(isNeedClone) {
            ValueType cloneObject = clone(value);
            if (cloneObject != null)
                innerPut(key, cloneObject);
        } else {
            innerPut(key,value);
        }
	}

	synchronized private void innerPut(KeyType key, ValueType value) {
		if (capability > 0) {
            Element element = lruCacheImpl.get(key);
            if (element == null) {
                while (lruCacheImpl.size() >= this.capability && lruCacheImpl.size() > 0) {
                    lruCacheImpl.remove(lruCacheImpl.keySet().iterator().next());
                }
                //这个new Element(null) 是什么鬼？
                lruCacheImpl.put(key, new Element(null));
            }else  {
                lruCacheImpl.put(key, new Element(value));
            }
        }
	}

	public ValueType get(KeyType key) {
		tryUndoEnhance();
		ValueType value = innerGet(key);
		if (value == null)
			return null;
        if(isNeedClone) {
            ValueType cloneObject = clone(value);
            return cloneObject;
        }else {
            return value;
        }
	}

	synchronized private ValueType innerGet(KeyType key) {
		Element element = lruCacheImpl.get(key);
		if (element == null) {
			return null;
		}
		long now = System.currentTimeMillis();

		long pastTime = now - element.getLastUpdateTime();
		//我震惊了，居然还有这种操作？getLastUpdateTime 应该会有什么操作吧？
		//getLastUpdateTime根本没操作
		if (pastTime >= expireTime) {
			// double check
			pastTime = now - element.getLastUpdateTime();
			if (pastTime >= expireTime) {
				// expired, update entry
				element.updateLastUpdateTime();
				return null;
			}
		}
		// element object value never null;
		return element.getValue();
	}



	private ValueType clone(ValueType obj) {
		ObjectInputStream is = null;
		ObjectOutputStream os = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			os = new ObjectOutputStream(bos);
			os.writeObject(obj);
			ByteArrayInputStream bin = new ByteArrayInputStream(
					bos.toByteArray());
			is = new TairObjectInputStream(bin, customClassLoader);
			ValueType clone = valueClass.cast(is.readObject());
			return clone;
		} catch (Exception ex) {
			//TODO log
		} finally {
			try {
				if (os != null)
					os.close();
				if (is != null)
					is.close();
			} catch (Exception ex) {
			}
		}
		return null;
	}

	public void enhance() {
		if (System.currentTimeMillis() - lastEnhanceTime < ENHANCE_INTERVAL) {
			return ;
		}
		lastEnhanceTime = System.currentTimeMillis();
		int delta_cap = capability / 10;
		long delta_exp = expireTime / 10;
		if (delta_cap == 0) {
			delta_cap = 1;
		}
		if (delta_cap + currentCapDelta > MAX_CAPABILITY_DELTA) {
			delta_cap = MAX_CAPABILITY_DELTA - currentCapDelta;
		}
		if (delta_exp == 0) {
			delta_exp = 1;
		}
		if (delta_exp + currentExpDelta > MAX_EXPIRETIME_DELTA) {
			delta_exp = MAX_EXPIRETIME_DELTA - currentExpDelta;
		}

		capability += delta_cap;
		expireTime += delta_exp;
		currentCapDelta += delta_cap;
		currentExpDelta += delta_exp;
		log.info("Do LRULocalCache Enhancement, expire: " + expireTime + ", capacity: " + capability);
	}

	private void tryUndoEnhance() {
		if ((currentCapDelta != 0 || currentExpDelta != 0)
				&& System.currentTimeMillis() - lastEnhanceTime > UNDO_ENHANCE_INTERVAL) {
			capability -= currentCapDelta;
			expireTime -= currentExpDelta;
			currentCapDelta = 0;
			currentExpDelta = 0;
			log.info("Undo LRULocalCache Enhancement, expire: " + expireTime + ", capacity: " + capability);
		}
	}

}
