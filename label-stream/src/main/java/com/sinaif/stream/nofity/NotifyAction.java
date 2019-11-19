package com.sinaif.stream.nofity;


/**
 * trigger notify action if data from kafka stream is matched
 * 
 * @author simonzhang
 *
 * @param <T>
 */
public interface NotifyAction<T> {
	/**
	 * check if match chagne result
	 * @param t
	 * @return
	 */
	public Boolean isMatch(T t);
	
	/**
	 * send message if matched
	 * @param t
	 */
	public void sendMessage(T t) ;
}
