package com.sinaif.stream.nofity;

import java.util.HashMap;
import java.util.Map;

public class NotifyRuleFactory {

	private static Map<String, NotifyAction<?>> NOTIRY_RULE_REPOSITORY = new HashMap<>();
	
	protected static void init() {
		//1. read from mysql 

		//2. put all into NOTIRY_RULE_REPOSITORY

	}
	
	@SuppressWarnings("unchecked")
	public static <T> void execute(String topic, String table, T payload) {
		
		NotifyAction<T> action = (NotifyAction<T>) NOTIRY_RULE_REPOSITORY.get(generateKey(topic, table));
		if (action.isMatch(payload)) {
			action.equals(payload);
		}
		
	}
	
	public static String generateKey(String topic, String table) {
		String key = topic+"_"+table;
		return key;
	}
	
	
}
