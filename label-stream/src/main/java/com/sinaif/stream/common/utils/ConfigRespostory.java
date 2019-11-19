package com.sinaif.stream.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.sinaif.stream.common.model.business.MysqlChangeLog;
import com.sinaif.stream.common.model.business.SinaifBocCreditSrc.SbcsBCustomerActionProductBase;
import com.sinaif.stream.common.model.business.SinaifCreditAddressSrc.ScasLCustomerBasicLabelsBase;
import com.sinaif.stream.common.model.business.SinaifCreditBankInfoSrc.SbcbsLCustomerFinanceLabelsBase;
import com.sinaif.stream.common.model.business.SinaifCreditIdCardSrc.SccsBCustomerInfoBase;
import com.sinaif.stream.common.model.business.SinaifCreditJobinfoSrc.ScjsLCustomerBasicLabelsBase;
import com.sinaif.stream.common.model.business.SinaifEasyDeviceSyncSrc.SedssBCustomerInfoBase;
import com.sinaif.stream.common.model.business.SinaifEasyUserAccountSrc.SeuasBCustomerInfoBase;
import com.sinaif.stream.common.model.business.SinaifKingBillInfoSrc.SkbsBCustomerActionProductBase;
import com.sinaif.stream.common.model.business.SinaifKingCreditInfoSrc.SkcsBCustomerActionProductBase;
import com.sinaif.stream.common.model.business.SinaifKingLoanProgressSrc.SklpsBCustomerActionProductBase;
import com.sinaif.stream.common.model.business.SinaifLoanProgressSrc.SlpsBCustomerActionProductBase;
import com.sinaif.stream.common.model.business.SinaifLoanRepaymentBaseSrc.SlrbsBCustomerActionProductBase;
import com.sinaif.stream.common.model.business.SinaifUserAccountSrc.SuasBCustomerInfoBase;
import com.sinaif.stream.common.model.business.SinaifUserProductrelSrc.SupsBCustomerActionProductBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigRespostory {

	private static Logger LOGGER = LoggerFactory.getLogger(ConfigRespostory.class);
	public static InputStream CONFIG_INPUT_STREAM;
	
	public static final Properties PROPERTIES = new Properties();
	
	public static final String _KAFKA_GROUP=".kafka.group.id";
	public static final String _KAFKA_TOPIC=".kafka.event.topic";
	public static final String _HISTORY_ENABLE=".history.enable";

	/**
	 * key is topic name
	 */
	@SuppressWarnings("rawtypes")
	private static Map<String, TypeRefSerialize> REF_MAP = new HashMap<>();
	
	static {
		init();

		REF_MAP.put("sinaif.t_credit_idcard", new TypeRefSerialize<MysqlChangeLog<SccsBCustomerInfoBase>>(){});
		REF_MAP.put("sinaif_easy.t_device_sync", new TypeRefSerialize<MysqlChangeLog<SedssBCustomerInfoBase>>(){});
		REF_MAP.put("sinaif_easy.t_user_account", new TypeRefSerialize<MysqlChangeLog<SeuasBCustomerInfoBase>>(){});
		REF_MAP.put("sinaif.t_user_account", new TypeRefSerialize<MysqlChangeLog<SuasBCustomerInfoBase>>(){});
		REF_MAP.put("sinaif.t_credit_jobinfo", new TypeRefSerialize<MysqlChangeLog<ScjsLCustomerBasicLabelsBase>>(){});
		REF_MAP.put("sinaif.t_credit_address", new TypeRefSerialize<MysqlChangeLog<ScasLCustomerBasicLabelsBase>>(){});
		REF_MAP.put("sinaif.t_user_productrel", new TypeRefSerialize<MysqlChangeLog<SupsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif.t_loan_progress", new TypeRefSerialize<MysqlChangeLog<SlpsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif_king.t_loan_progress", new TypeRefSerialize<MysqlChangeLog<SklpsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif.t_boc_credit", new TypeRefSerialize<MysqlChangeLog<SbcsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif_king.t_credit_info", new TypeRefSerialize<MysqlChangeLog<SkcsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif.t_loan_repayment_base", new TypeRefSerialize<MysqlChangeLog<SlrbsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif_king.t_bill_info", new TypeRefSerialize<MysqlChangeLog<SkbsBCustomerActionProductBase>>(){});
		REF_MAP.put("sinaif.t_credit_bankinfo", new TypeRefSerialize<MysqlChangeLog<SbcbsLCustomerFinanceLabelsBase>>(){});

	}

	public static void init() {
		CONFIG_INPUT_STREAM = ConfigRespostory.class.getResourceAsStream("/application.properties");
		try {
			PROPERTIES.load(CONFIG_INPUT_STREAM);
		} catch (IOException e) {
			LOGGER.error(e.toString());
		}

		CommonConfig.KAFKA_BROKERS = PROPERTIES.getProperty("kafka.brokers");
		CommonConfig.KAFKA_ZOOKEEPER_CONNECT = PROPERTIES.getProperty("kafka.zookeeper.connect");
		CommonConfig.STREAM_PARALLELISM = PROPERTIES.getProperty("stream.parallelism");
		CommonConfig.STREAM_SINK_PARALLELISM = PROPERTIES.getProperty("stream.sink.parallelism");
		CommonConfig.STREAM_DEFAULT_PARALLELISM = PROPERTIES.getProperty("stream.default.parallelism");
		CommonConfig.STREAM_CHECKPOINT_ENABLE = PROPERTIES.getProperty("stream.checkpoint.enable");
		CommonConfig.STREAM_CHECKPOINT_INTERVAL = PROPERTIES.getProperty("stream.checkpoint.interval");
	}

	public static String  value(String key) {
		return PROPERTIES.getProperty(key);
	}
	
	
	public static class CommonConfig {
		public static String KAFKA_BROKERS;
		public static String KAFKA_ZOOKEEPER_CONNECT;

		public static String STREAM_PARALLELISM;
		public static String STREAM_SINK_PARALLELISM;
		public static String STREAM_DEFAULT_PARALLELISM;
		public static String STREAM_CHECKPOINT_ENABLE;
		public static String STREAM_CHECKPOINT_INTERVAL;
		
	}

	public static String get(String key) {
		return PROPERTIES.getProperty(key);
	}

	public static TypeRefSerialize<?> ref(String topic_type) {
		return REF_MAP.get(topic_type);
	}

}
