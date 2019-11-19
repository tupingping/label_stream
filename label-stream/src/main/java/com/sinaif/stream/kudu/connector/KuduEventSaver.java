package com.sinaif.stream.kudu.connector;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kudu.client.*;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.sinaif.stream.common.model.AutoChange;
import com.sinaif.stream.common.model.operator.UserCall;
import com.sinaif.stream.common.utils.ConfigRespostory;

/**
 * 
 * save into kudu database
 * 
 * @author simonzhang
 *
 */
public class KuduEventSaver {

	private static final Logger LOGGER = LoggerFactory.getLogger(KuduEventSaver.class);

	private static KuduClient KUDU_CLIENT;
	private static KuduSession KUDU_SESSION;
	static {
		init();
	}

	private static void init() {
		//TODO  get master address from config file
		String host = ConfigRespostory.value("kudu.master.host");
		KUDU_CLIENT = new KuduClient.KuduClientBuilder(host).build();
		KUDU_SESSION = KUDU_CLIENT.newSession();
		KUDU_SESSION.setFlushMode(FlushMode.MANUAL_FLUSH);
//		KUDU_SESSION.setFlushInterval(Integer.parseInt(ConfigRespostory.get("kudu.commit.time.interval"))); // 1s
//		KUDU_SESSION.setMutationBufferSpace(Integer.parseInt(ConfigRespostory.get("kudu.commit.maxrows"))); // 200 rows
	}

	private static Map<String, KuduTable> TABLE_MAP = new ConcurrentHashMap<String, KuduTable>();

	public static void saveOrUpdateRecord(Object payload, WriteMode mode) {
		try {

			Tuple2<String, Set<KuduColumnInfo>> tuple2 = generateInfos(payload);

			Operation operator = toOperation(table(tuple2.f0), mode);
			PartialRow row = operator.getRow();

			for (KuduColumnInfo info : tuple2.f1) {
				KuduMapper.toOperation(row, info.columnName, info.value, info.type);
			}

			session().apply(operator);
			List<OperationResponse> results = session().flush();
			for (OperationResponse result : results) {
				if(result.hasRowError())
					// TODO: insert into error where pk is the same.
					if(!result.getRowError().getErrorStatus().isAlreadyPresent()){
						LOGGER.error(result.getRowError().toString());
					}
			}
		} catch (Exception e) {
			LOGGER.error("Save data to kudu database exception. payload:{}. Exception:{}", payload, e);
		}
	}

	public enum WriteMode {INSERT, UPDATE, UPSERT, DELETE}
	
	private static Operation toOperation(KuduTable table, WriteMode writeMode) {
		switch (writeMode) {
		case INSERT:
			return table.newInsert();
		case UPDATE:
			return table.newUpdate();
		case UPSERT:
			return table.newUpsert();
		case DELETE:
			return table.newDelete();
		}
		
		return table.newUpsert();
	}

	private static Tuple2<String, Set<KuduColumnInfo>> generateInfos(Object payload) {

		KuduDomain domain = payload.getClass().getAnnotation(KuduDomain.class);
		if (domain == null) {
			LOGGER.error("class {} need to be declared by {}", payload.getClass().getName(), KuduDomain.class.getName());
			return null;
		}
		
		try {
			Method method = payload.getClass().getMethod(AutoChange.METHOD);
			method.invoke(payload);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}

		Set<KuduColumnInfo> infos = new HashSet<>();
		parse(payload, payload.getClass(), infos);

		return new Tuple2<>(domain.table(), infos);
	}
	
	private static Set<KuduColumnInfo> parse(Object payload, Class<?> clazz, Set<KuduColumnInfo> infos){
		// fields in domain self
		Class<?> superClass = clazz.getSuperclass();
		if (superClass != null && !superClass.equals(Object.class)) {
			parse(payload, superClass, infos);
			
		}

		Field[] fields = clazz.getDeclaredFields();
		
		for (Field field : fields) {

			if (field.isAnnotationPresent(KuduColumnIgnore.class)) {
				continue;
			}

			if ( Modifier.isStatic(field.getModifiers()) ) {
				continue;
			}
			
			KuduColumnInfo columnInfo = new KuduColumnInfo();
			KuduColumn kuduColumn = field.getAnnotation(KuduColumn.class);
			columnInfo.columnName = kuduColumn != null &&  !StringUtils.isEmpty(kuduColumn.property()) ? 
					kuduColumn.property() :  field.getName()  ;
			columnInfo.type =  kuduColumn != null && !StringUtils.isEmpty(kuduColumn.type()) ?
					kuduColumn.type() :  field.getType().getSimpleName() ;
			try {
				columnInfo.value = field.get(payload);
			} catch (Exception e) {
				LOGGER.error(columnInfo.columnName, e);
			}

			infos.add(columnInfo);
		}
		return infos;
	}
	


	private static KuduSession session() {
		if (KUDU_SESSION != null && !KUDU_SESSION.isClosed()) {
			return KUDU_SESSION;
		}
		KUDU_SESSION = KUDU_CLIENT.newSession();
		return KUDU_SESSION;
	}

	private static KuduTable table(String name) {
		KuduTable table = TABLE_MAP.get(name);
		if (table == null) {
			try {

				table = KUDU_CLIENT.openTable(name);
			} catch (KuduException e) {
				LOGGER.error(String.format("table {1} don't exist", name), e);
			}

			TABLE_MAP.put(name, table);
		}
		return table;
	}

	public static void main(String[] payload) {
		String json = "{\"cell_phone\":\"123\", \"other_cell_phone\":\"345\", \"userid\":\"678\", \"start_time\":\"2019-07-13 19:05:01\", \"subtotal\":38, \"place\":\"B\", \"init_type\":\"B\", \"call_type\":\"B\", \"use_time\": 345}";
		UserCall uc = JSON.parseObject(json, UserCall.class);
//		Date month = DateUtils.truncate(uc.start_time, Calendar.MONTH);
//		System.out.println(month);
//		System.out.println(uc);
//		System.out.println(ReflectionToStringBuilder.reflectionToString(uc, ToStringStyle.MULTI_LINE_STYLE));
		
		Tuple2<String, Set<KuduColumnInfo>> infos = generateInfos(uc);
		System.out.println(infos);
	}

}
