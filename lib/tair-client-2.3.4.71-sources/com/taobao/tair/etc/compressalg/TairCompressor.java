package com.taobao.tair.etc.compressalg;

public interface TairCompressor {

	/**
	 * 压缩数据
	 *
	 * @param in
	 *			压缩之前的原生数据
	 * @return
	 *			压缩后的数据
	 */
	public byte[] compress(byte[] in);

	/**
	 * 解压数据
	 *
	 * @param in
	 *			解压之前的原生数据
	 * @return
	 *			解压后的数据
	 */
	public byte[] decompress(byte[] in);
}
