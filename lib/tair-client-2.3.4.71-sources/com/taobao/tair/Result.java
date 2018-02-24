/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair;

import java.util.Collection;
import java.io.Serializable;

import com.taobao.tair.etc.TairConstant;



/**
 * CacheResult object return by tair server
 */
public class Result<V> implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ResultCode rc;
	private V value;
    private short flag;

	public static final short FLAG_HASNEXT = 0x01;

	public Result(ResultCode rc) {
		this.rc = rc;
	}

	public Result(ResultCode rc, V value) {
		this.rc = rc;
		this.value = value;
		this.flag = 0;
	}

	public Result(ResultCode rc, V value, short flag) {
		this.rc = rc;
		this.value = value;
		this.flag = flag;
	}

	/**
	 * whether the request is success.
	 * <p>
	 * if the target is not exist, this method return true.
	 */
	public boolean isSuccess() {
		return rc.isSuccess();
	}

	public V getValue() {
		return this.value;
	}

    public boolean hasNext(){
        return (this.flag & FLAG_HASNEXT)!= 0;
    }

    public short getFlag(){
        return flag;
    }

    public boolean isLocked() {
		return (this.flag  & TairConstant.TAIR_ITEM_FLAG_LOCKED) != 0;
	}

	public boolean isCounter() {
		return (this.flag & TairConstant.TAIR_ITEM_FLAG_ADDCOUNT) != 0;
	}

	public boolean isHidden() {
		return (this.flag & TairConstant.TAIR_ITEM_FLAG_DELETED) != 0;
	}
	
	public boolean isNeglected() {
		return (this.flag & TairConstant.TAIR_ITEM_FLAG_NEGLECTED) != 0;
	}

	/**
	 * @return the result code of this request
	 */
	public ResultCode getRc() {
		return rc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("CacheResult: [").append(rc.toString()).append("]\n");
		if(value != null) {
			if(value instanceof DataEntry) {
				sb.append("\t").append(value.toString()).append("\n");
			} else if (value instanceof Collection) {
				Collection<DataEntry> des = (Collection<DataEntry>) value;
				sb.append("\tentry size: ").append(des.size()).append("\n");
				for (DataEntry de : des) {
					sb.append("\t").append(de.toString()).append("\n");
				}
			} else {
				sb.append("\tvalue: ").append(value);
			}
		}
		return sb.toString();
	}
}
