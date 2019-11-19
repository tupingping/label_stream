package com.sinaif.stream.common.model.operator;

import org.apache.flink.api.java.functions.KeySelector;

public class UserCallKeySelection implements KeySelector<UserCall, UserCall>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4566388917240989269L;

	public static final UserCallKeySelection SELECTIOR = new UserCallKeySelection();

	@Override
	public UserCall getKey(UserCall value) throws Exception {
		// TODO Auto-generated method stub
		return value;
	}
	
//	@Override
//	public UserCall getKey(Event value) throws Exception {
//		UserCall key = (UserCall) value.playload;
//		
//		return key.isWrong() ? UserCall.WRONG_KEY : key;
//	}

	

}
