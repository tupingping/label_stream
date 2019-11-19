package com.sinaif.stream.common.utils;

import java.math.BigDecimal;

public class DataUtil {

	public static Integer add(Object v1, Object v2) {
		return new BigDecimal(v1.toString()).add(new BigDecimal(v2.toString())).intValue();
	}
}
