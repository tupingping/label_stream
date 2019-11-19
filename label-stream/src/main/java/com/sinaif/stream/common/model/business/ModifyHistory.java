package com.sinaif.stream.common.model.business;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sinaif.stream.common.model.AutoChange;
import com.sinaif.stream.common.utils.HashUtil;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KuduDomain(table = "impala:stream_test.b_customer_updates")
public class ModifyHistory implements AutoChange {

	private static Logger LOGGER = LoggerFactory.getLogger(ModifyHistory.class);

	@KuduColumn(property = "customer_pk_id")
	public Long customerId;

	public Date update_time;

	public Long table_name_hash;

	public Long terminal_customer_id;

	public Long terminal_code;

	public String table_name;

	public String database;

	public String main_change;

	public String operation;
	
	public String change_content;
	
	public Date  update_dt;

	@Override
	public void autochange() {
		update_dt = update_time;
	}

	public static ModifyHistory generate(MysqlChangeLog<?> log, String type) {
		ModifyHistory history = new ModifyHistory();
		history.customerId = log.data.getCustomerId();
		history.update_time = new Date(log.ts*1000L);
		history.table_name = log.table;
		history.database = log.database;
		history.table_name_hash = Long.valueOf(HashUtil.hash(history.database + history.table_name));

		// history.terminal_customer_id

		// history.terminal_code

		String tmp = getMainChange(log);
		if(tmp.length() > 0)
			history.main_change = tmp;

		history.operation = type;
		history.change_content = JSON.toJSONString(log.data);

		return history;
	}

	private static String getMainChange(MysqlChangeLog<?> log){
		JSONObject data = new JSONObject();
		if(log.old != null){
			Field[] fields = log.old.getClass().getDeclaredFields();
			for(Field f : fields){
				try{
					Object oldValue = f.get(log.old);
					if(oldValue != null){
						String key = f.getName();

						oldValue = oldValue.toString();

						String newValue = log.data.getClass().getField(key).get(log.data).toString();
						String value = oldValue + "@" + newValue;

						data.put(key, value);
					}
				}catch (Exception e){
					LOGGER.error(e.toString());
					continue;
				}

			}

			return data.toJSONString();

		}

		return "";
	}
}
