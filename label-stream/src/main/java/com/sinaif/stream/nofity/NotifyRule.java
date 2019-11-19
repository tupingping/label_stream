package com.sinaif.stream.nofity;

import org.mvel2.MVEL;

public class NotifyRule<T> implements NotifyAction<T>{

	public String topicType;
	
	public String table;
	
	public String expression;
	
	public String outMessage;
	
	public String outConnection;
	
	
	public Boolean isMatch(T t) {
		return MVEL.evalToBoolean(expression, t);
	}
	
	public void sendMessage(T t) {
		//1 convert message using outMessage and t
		
		//2 send message to outConnection
		
	}
}
