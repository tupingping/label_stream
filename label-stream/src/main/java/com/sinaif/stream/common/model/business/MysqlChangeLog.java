package com.sinaif.stream.common.model.business;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class MysqlChangeLog<T extends CommonBusiness> implements Serializable {
	

	private static final long serialVersionUID = 1L;

	public String database;

	public String table;

	public String type;

	public Long ts;
	
	public T data;

	public T old;

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

	
}