package com.sinaif.stream.common.model.operator;

import java.lang.reflect.Field;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

import com.alibaba.fastjson.JSON;
import com.sinaif.stream.common.model.DomainObject;
import com.sinaif.stream.common.utils.AESToLong;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;

@KuduDomain(table = "impala:sinaif_tags:m_user_call")
public class UserCall extends DomainObject implements MapFunction<UserCall, UserCall> {
	// ---------------------primary key------------------
	@KuduColumn(property = "call_customer_id")
	public Long callCustomerId;
	@KuduColumn(property = "called_customer_id")
	public Long calledCustomerId;
	@KuduColumn(property = "month")
	public Date month;
	@KuduColumn
	public String call_type = "主叫";
	@KuduColumn
	public Long batchId;
	// --------------------------------------------
	@KuduColumn
	public String cell_phone;
	@KuduColumn
	public String other_cell_phone;
	@KuduColumn
	public Date start_time;
	@KuduColumn
	public int use_time;
	@KuduColumn
	public String init_type;
	@KuduColumn
	public String place;
	@KuduColumn
	public String userid;

	// ---------------analysis field--------------
	public short times_h_1;
	public short times_h_2;
	public short times_h_3;
	public short times_h_4;
	public short times_h_5;
	public short times_h_6;
	public short times_h_7;
	public short times_h_8;

	public short section_h_1;
	public short section_h_2;
	public short section_h_3;
	public short section_h_4;
	public short section_h_5;
	public short section_h_6;
	public short section_h_7;
	public short section_h_8;

	/**
	 * 本次通话花费, 精确到分
	 */
	@KuduColumn(property = "total_fees")
	public int subtoal;
	@KuduColumn(property = "call_times")
	public int callTimes = 1;

	// TODO
	public static final UserCall WRONG_KEY = new UserCall();

	public UserCall() {
	}

	public UserCall(String cell_phone, String other_cell_phone, Date month, String call_type) {
		this.cell_phone = cell_phone;
		this.other_cell_phone = other_cell_phone;
		this.month = month;
		this.call_type = call_type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cell_phone == null) ? 0 : cell_phone.hashCode());
		result = prime * result + ((other_cell_phone == null) ? 0 : other_cell_phone.hashCode());
		result = prime * result + ((month == null) ? 0 : month.hashCode());
		result = prime * result + ((call_type == null) ? 0 : call_type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserCall other = (UserCall) obj;
		if (cell_phone == null) {
			if (other.cell_phone != null)
				return false;
		} else if (!cell_phone.equals(other.cell_phone))
			return false;
		if (other_cell_phone == null) {
			if (other.other_cell_phone != null)
				return false;
		} else if (!other_cell_phone.equals(other.other_cell_phone))
			return false;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		if (call_type == null) {
			if (other.call_type != null)
				return false;
		} else if (!call_type.equals(other.call_type))
			return false;
		return true;
	}

	public void convert() {
		String call = call_type.contains("主") ? cell_phone : other_cell_phone;

		if (StringUtils.isNotBlank(call)) {
			callCustomerId = AESToLong.convertToNumber(call);
		}

		String called = call_type.contains("主") ? other_cell_phone : cell_phone;
		if (StringUtils.isNotBlank(called)) {
			calledCustomerId = AESToLong.convertToNumber(called);
		}

		month = DateUtils.truncate(this.start_time, Calendar.MONTH);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(this.start_time);
		int hour = calendar.get(Calendar.HOUR_OF_DAY) / 3;
		try {
			Field times = this.getClass().getField("times_h_" + hour);
			times.set(this, (short) 1);
			Field section = this.getClass().getField("section_h_" + hour);
			section.set(this, (short) use_time);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void analysisByHour(UserCall source) {
		for (int index = 1; index < 9; index++) {
			try {
				Field times_t = this.getClass().getField("times_h_" + index);
				Field times_s = source.getClass().getField("times_h_" + index);
				Short times = (short) ((Short) times_t.get(this) + (Short) times_s.get(source));
				times_t.set(this, times);

				Field section_t = this.getClass().getField("section_h_" + index);
				Field section_s = source.getClass().getField("section_h_" + index);
				Short section = (short) ((Short) section_t.get(this) + (Short) section_s.get(source));
				section_t.set(this, section);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isWrong() {
		return cell_phone == null || other_cell_phone == null || month == null;
	}

	public static void main(String[] args) {
		String json = "{\"cell_phone\":\"123\", \"other_cell_phone\":\"345\", \"userid\":\"678\", \"start_time\":\"2019-07-13 19:05:01\", \"subtotal\":38, \"place\":\"B\", \"init_type\":\"B\", \"call_type\":\"B\", \"use_time\": 345}";
		UserCall us = JSON.parseObject(json, UserCall.class);
		Date month = DateUtils.truncate(us.start_time, Calendar.MONTH);
		System.out.println(month);
		System.out.println(us);
		System.out.println(ReflectionToStringBuilder.reflectionToString(us, ToStringStyle.MULTI_LINE_STYLE));
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}

	@Override
	public void autochange() {
		super.autochange();
		this.batchId = System.currentTimeMillis();
	}

	@Override
	public UserCall map(UserCall value) throws Exception {
		value.convert();
		return value;
	}
}
