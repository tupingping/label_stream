package com.sinaif.stream.common.model;

/**
 * auto change all domain field
 * 
 * @author simonzhang
 *
 */
public interface AutoChange {

	/**
	 * must same with the method
	 */
	public static final String  METHOD = "autochange";
	/**
	 * domain fields change method
	 */
	public void autochange();
}
