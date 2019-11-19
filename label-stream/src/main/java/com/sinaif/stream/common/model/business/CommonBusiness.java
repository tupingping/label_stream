package com.sinaif.stream.common.model.business;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.fastjson.annotation.JSONField;
import com.sinaif.stream.common.model.AutoChange;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduEventSaver.WriteMode;

public abstract class CommonBusiness implements AutoChange, Serializable {

	private static final long serialVersionUID = 1L;

	public Date update_dt;

	@JSONField(serialize = false, deserialize = false)
	public Short flag_del = 0;

	@KuduColumn(property = "customer_pk_id")
	public Long customerId;

	@JSONField(serialize = false, deserialize = false)
	@KuduColumnIgnore
	public Boolean enableHistory = true;

	@JSONField(serialize = false, deserialize = false)
	@KuduColumnIgnore
	public MysqlChangeLog<?> log;

	@JSONField(serialize = false, deserialize = false)
	@KuduColumnIgnore
	public String rawType;

	@Override
	public void autochange() {
		update_dt = new Date(log.ts*1000L);
	}

	/**
	 * filter invalid data
	 * @return
	 */
	public Boolean filter(){
		return true;
	}

	/**
	 * 
	 * @return kudu mode
	 */
	public WriteMode mode() {

		WriteMode mode = null;
		switch(log.type.toLowerCase()) {
			case "insert":
				mode = WriteMode.INSERT; break;
			case "update":
				mode = WriteMode.UPSERT; break;
			case "delete":
				mode = WriteMode.DELETE; break;
			default:
				mode = WriteMode.UPSERT;
		}
		return mode;

	}

	public void TypeConventer(){
		rawType = log.type;
		if(log.type.equals("insert"))
			log.type = "update";
		else if(log.type.equals("delete")){
			log.type = "update";
			flag_del = 1;
		}
	}
	/**
	 * get data
	 * @return
	 */
	public Tuple2<Object, KuduEventSaver.WriteMode> get(Object src) {
		this.prepare(src);
		Tuple2<Object, KuduEventSaver.WriteMode> result = new Tuple2<>(this, mode());
		return result;
	}

	/**
	 *
	 */
	public abstract void prepare(Object src);

	/**
	 *
	 * @return
	 */
	public Long getCustomerId(){ return customerId; }


	public List<Tuple2<Object, WriteMode>> convert(){
		return null;
	}

}
