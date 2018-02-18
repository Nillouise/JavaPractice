package com.taobao.tair;

import java.io.Serializable;


public abstract class DataEntryAbstract<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected T value;
	
	public T getValue() {
		return value;
	}
	
	protected void setValue(T value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(value);
		return sb.toString();
	}
}
