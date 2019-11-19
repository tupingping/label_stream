package com.sinaif.stream.common.model;

import java.util.Date;
import java.util.Map;

public class Event {

	/**
	 * Metric name
	 */
	public String name;

	/**
	 * Metric timestamp
	 */
	public Long timestamp = new Date().getTime();

	/**
	 * Metric fields
	 */
	public Object playload;

	/**
	 * Metric tags
	 */
	public Map<String, String> tags;
	
	@Override
	public String toString() {
		return playload.toString();
	}
	
	public <T> Object getPlayload() {
		return (T)playload;
	}
}
