/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair.comm;

public interface Transcoder {

    byte[] encode(Object object);
    byte[] encode(Object object, boolean prefixCared);

    Object decode(byte[] data);

    Object decode(byte[] data, int offset, int size);
    Object decode(byte[] data, int offset, int size, int prefixSize);

	String getCharset();
}
