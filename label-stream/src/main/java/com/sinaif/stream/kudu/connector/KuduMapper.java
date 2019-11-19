package com.sinaif.stream.kudu.connector;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.kudu.client.PartialRow;

final class KuduMapper {

	public static void toOperation(PartialRow partialRow, String columnName, Object value, String type) {
		if(value != null){
			switch (type) {
				case "String":
					partialRow.addString(columnName, (String) value);
					break;
				case "Float":
					partialRow.addFloat(columnName, (Float) value);
					break;
				case "Byte":
					partialRow.addByte(columnName, (Byte) value);
					break;
				case "Short":
					partialRow.addShort(columnName, (Short) value);
					break;
				case "Integer":
					partialRow.addInt(columnName, (Integer) value);
					break;
				case "Long":
					partialRow.addLong(columnName, (Long) value);
					break;
				case "Double":
					partialRow.addDouble(columnName, (Double) value);
					break;
				case "Boolean":
					partialRow.addBoolean(columnName, (Boolean) value);
					break;
				case "Date":
					Date date = (Date) value;
					Timestamp timestamp = new Timestamp(date.getTime());
					partialRow.addTimestamp(columnName, timestamp);
					break;
				default:
					throw new IllegalArgumentException("Illegal var type: " + type);
			}
		}
	}

}
