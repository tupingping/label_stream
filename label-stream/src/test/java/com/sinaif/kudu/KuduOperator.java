package com.sinaif.kudu;

import com.sinaif.stream.common.utils.ConfigRespostory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @File : KuduOperator.java
 * @Author: tupingping
 * @Date : 2019/8/8
 * @Desc :
 */
public class KuduOperator {
    private static final Logger LOG = LoggerFactory.getLogger(KuduOperator.class);

    private static KuduClient KUDU_CLIENT;
    private static KuduSession KUDU_SESSION;

    private KuduTableCol kuduTableCol = new KuduTableCol();

    static {
        init();
    }

    private static void init() {
        String host = ConfigRespostory.value("kudu.master.host");
        KUDU_CLIENT = new KuduClient.KuduClientBuilder(host).build();
        KUDU_SESSION = KUDU_CLIENT.newSession();
        KUDU_SESSION.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        // KUDU_SESSION.setFlushInterval(10000);
        // KUDU_SESSION.setMutationBufferSpace(2000);
    }

    private static KuduSession session() {
        if (KUDU_SESSION != null && !KUDU_SESSION.isClosed()) {
            return KUDU_SESSION;
        }
        KUDU_SESSION = KUDU_CLIENT.newSession();
        return KUDU_SESSION;
    }

    public void CreateTable(){
        this.kuduTableCol.setTableName("impala::sinaif_label.b_customer_info");

        List cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("customer_pk_id", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_card_name", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_cardno", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("phone_num", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("birthday", Type.INT32).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("sex", Type.INT16).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("race", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("webcaht_id", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("graduate_code", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_time", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("flag_del", Type.INT16).defaultValue(Short.valueOf("0")).build());

        this.kuduTableCol.setTableSchema(cols);

        List rangeKeys = new ArrayList();
        rangeKeys.add("customer_pk_id");
        this.kuduTableCol.setTableBuilder(rangeKeys);

        try{
            if(! KUDU_CLIENT.tableExists(kuduTableCol.getTableName())){
                KuduTable table = KUDU_CLIENT.createTable(kuduTableCol.getTableName(), kuduTableCol.getTableSchema(), new CreateTableOptions().setRangePartitionColumns(kuduTableCol.getTableBuilder()));
            }
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public void ScanRow(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("impala::sinaif_label.b_customer_info");
            KuduScanner scanner = KUDU_CLIENT.newScannerBuilder(table).build();
            int sum = 0;
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    sum += 1;
                    System.out.print(String.format("b_customer_info: 第 %d 条：", sum));

                    System.out.print("customer_pk_id: " + row.getLong("customer_pk_id"));

                    if(! row.isNull("idc_card_name"))
                        System.out.print("\t idc_card_name: " + row.getString("idc_card_name"));

                    if(! row.isNull("idc_cardno"))
                        System.out.print("\t idc_cardno: " + row.getString("idc_cardno"));

                    if(! row.isNull("phone_num"))
                        System.out.print("\t phone_num: " + row.getString("phone_num"));

                    if(! row.isNull("birthday"))
                        System.out.print("\t birthday: " + row.getInt("birthday"));

                    if(! row.isNull("sex"))
                        System.out.print("\t sex: " + row.getShort("sex"));

                    if(! row.isNull("race"))
                        System.out.print("\t race: " + row.getString("race"));

                    if(! row.isNull("webcaht_id"))
                        System.out.print("\t webcaht_id: " + row.getString("webcaht_id"));

                    if(! row.isNull("graduate_code"))
                        System.out.print("\t graduate_code: " + row.getString("graduate_code"));

                    if(! row.isNull("create_time"))
                        System.out.print("\t create_time: " + row.getTimestamp("create_time"));

                    if(! row.isNull("update_dt"))
                        System.out.print("\t update_dt: " + row.getTimestamp("update_dt"));

                    if(! row.isNull("create_dt"))
                        System.out.print("\t create_dt: " + row.getTimestamp("create_dt"));

                    if(! row.isNull("flag_del"))
                        System.out.print("\t flag_del: " + row.getShort("flag_del"));

                    System.out.println();
                }
            }
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void Insert(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("test");
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addLong(0, 21);
            row.addString(1, "ttttt222");
            session().apply(insert);
            List<OperationResponse> o = session().flush();
            System.out.println(o);
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void CreateTable1(){
        this.kuduTableCol.setTableName("impala::sinaif_label.l_customer_basic_labels");

        List cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("customer_pk_id", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_card_name", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_cardno_province", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_cardno_code", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_cardno_city", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_cardno_area", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_card_address", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_card_valid_start_date", Type.INT64).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_card_valid_end_date", Type.INT64).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_agency", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_cardno", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_zodiac", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_birthday", Type.INT32).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_constellation", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_sex", Type.INT16).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("idc_race", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("ua_phone_num", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("ua_phone_province", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("ua_phone_city", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("ua_phone_carrier", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("ua_phone_areacode", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("ua_phone_zipcode", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_incomecode", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_marriage", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_degreecode", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_clienttype", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_email", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_graduationyear", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_graduationschool", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_learningform", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_home_province", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_home_city", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_home_area", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_home_addr", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_province", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_city", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_area", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_addr", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_name", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_zone", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_company_telephone", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("credit_comp_extension_num", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("app_top1_name", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("app_top2_name", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("app_top3_name", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("app_top1_count", Type.INT64).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("app_top2_count", Type.INT64).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("app_top3_count", Type.INT64).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("flag_del", Type.INT16).defaultValue(Short.valueOf("0")).build());

        this.kuduTableCol.setTableSchema(cols);

        List rangeKeys = new ArrayList();
        rangeKeys.add("customer_pk_id");
        this.kuduTableCol.setTableBuilder(rangeKeys);

        try{
            if(! KUDU_CLIENT.tableExists(kuduTableCol.getTableName())){
                KuduTable table = KUDU_CLIENT.createTable(kuduTableCol.getTableName(), kuduTableCol.getTableSchema(), new CreateTableOptions().setRangePartitionColumns(kuduTableCol.getTableBuilder()));
            }
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public void ScanRow1(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("impala::sinaif_label.l_customer_basic_labels");
            KuduScanner scanner = KUDU_CLIENT.newScannerBuilder(table).build();
            int sum = 0;
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    sum += 1;
                    System.out.print(String.format("l_customer_basic_labels: 第 %d 条：", sum));

                    System.out.print("customer_pk_id: " + row.getLong("customer_pk_id"));

                    if(! row.isNull("idc_card_name"))
                        System.out.print("\t idc_card_name: " + row.getString("idc_card_name"));

                    if(! row.isNull("idc_cardno_province"))
                        System.out.print("\t idc_cardno_province: " + row.getString("idc_cardno_province"));

                    if(! row.isNull("idc_cardno_code"))
                        System.out.print("\t idc_cardno_code: " + row.getString("idc_cardno_code"));

                    if(! row.isNull("idc_cardno_city"))
                        System.out.print("\t idc_cardno_city: " + row.getString("idc_cardno_city"));

                    if(! row.isNull("idc_cardno_area"))
                        System.out.print("\t idc_cardno_area: " + row.getString("idc_cardno_area"));

                    if(! row.isNull("idc_card_address"))
                        System.out.print("\t idc_card_address: " + row.getString("idc_card_address"));

                    if(! row.isNull("idc_card_valid_start_date"))
                        System.out.print("\t idc_card_valid_start_date: " + row.getLong("idc_card_valid_start_date"));

                    if(! row.isNull("idc_card_valid_end_date"))
                        System.out.print("\t idc_card_valid_end_date: " + row.getLong("idc_card_valid_end_date"));

                    if(! row.isNull("idc_agency"))
                        System.out.print("\t idc_agency: " + row.getString("idc_agency"));

                    if(! row.isNull("idc_cardno"))
                        System.out.print("\t idc_cardno: " + row.getString("idc_cardno"));

                    if(! row.isNull("idc_zodiac"))
                        System.out.print("\t idc_zodiac: " + row.getString("idc_zodiac"));

                    if(! row.isNull("idc_birthday"))
                        System.out.print("\t idc_birthday: " + row.getInt("idc_birthday"));

                    if(! row.isNull("idc_constellation"))
                        System.out.print("\t idc_constellation: " + row.getString("idc_constellation"));

                    if(! row.isNull("idc_sex"))
                        System.out.print("\t idc_sex: " + row.getShort("idc_sex"));

                    if(! row.isNull("idc_race"))
                        System.out.print("\t idc_race: " + row.getString("idc_race"));

                    if(! row.isNull("ua_phone_num"))
                        System.out.print("\t ua_phone_num: " + row.getString("ua_phone_num"));

                    if(! row.isNull("ua_phone_province"))
                        System.out.print("\t ua_phone_province: " + row.getString("ua_phone_province"));

                    if(! row.isNull("ua_phone_city"))
                        System.out.print("\t ua_phone_city: " + row.getString("ua_phone_city"));

                    if(! row.isNull("ua_phone_carrier"))
                        System.out.print("\t ua_phone_carrier: " + row.getString("ua_phone_carrier"));

                    if(! row.isNull("ua_phone_areacode"))
                        System.out.print("\t ua_phone_areacode: " + row.getString("ua_phone_areacode"));

                    if(! row.isNull("ua_phone_zipcode"))
                        System.out.print("\t ua_phone_zipcode: " + row.getString("ua_phone_zipcode"));

                    if(! row.isNull("credit_incomecode"))
                        System.out.print("\t credit_incomecode: " + row.getString("credit_incomecode"));

                    if(! row.isNull("credit_marriage"))
                        System.out.print("\t credit_marriage: " + row.getString("credit_marriage"));

                    if(! row.isNull("credit_degreecode"))
                        System.out.print("\t credit_degreecode: " + row.getString("credit_degreecode"));

                    if(! row.isNull("credit_clienttype"))
                        System.out.print("\t credit_clienttype: " + row.getString("credit_clienttype"));

                    if(! row.isNull("credit_email"))
                        System.out.print("\t credit_email: " + row.getString("credit_email"));

                    if(! row.isNull("credit_graduationyear"))
                        System.out.print("\t credit_graduationyear: " + row.getString("credit_graduationyear"));

                    if(! row.isNull("credit_graduationschool"))
                        System.out.print("\t credit_graduationschool: " + row.getString("credit_graduationschool"));

                    if(! row.isNull("credit_learningform"))
                        System.out.print("\t credit_learningform: " + row.getString("credit_learningform"));

                    if(! row.isNull("credit_home_province"))
                        System.out.print("\t credit_home_province: " + row.getString("credit_home_province"));

                    if(! row.isNull("credit_home_city"))
                        System.out.print("\t credit_home_city: " + row.getString("credit_home_city"));

                    if(! row.isNull("credit_home_area"))
                        System.out.print("\t credit_home_area: " + row.getString("credit_home_area"));

                    if(! row.isNull("credit_home_addr"))
                        System.out.print("\t credit_home_addr: " + row.getString("credit_home_addr"));

                    if(! row.isNull("credit_company_province"))
                        System.out.print("\t credit_company_province: " + row.getString("credit_company_province"));

                    if(! row.isNull("credit_company_city"))
                        System.out.print("\t credit_company_city: " + row.getString("credit_company_city"));

                    if(! row.isNull("credit_company_area"))
                        System.out.print("\t credit_company_area: " + row.getString("credit_company_area"));

                    if(! row.isNull("credit_company_addr"))
                        System.out.print("\t credit_company_addr: " + row.getString("credit_company_addr"));

                    if(! row.isNull("credit_company_name"))
                        System.out.print("\t credit_company_name: " + row.getString("credit_company_name"));

                    if(! row.isNull("credit_company_zone"))
                        System.out.print("\t credit_company_zone: " + row.getString("credit_company_zone"));

                    if(! row.isNull("credit_company_telephone"))
                        System.out.print("\t credit_company_telephone: " + row.getString("credit_company_telephone"));

                    if(! row.isNull("credit_comp_extension_num"))
                        System.out.print("\t credit_comp_extension_num: " + row.getString("credit_comp_extension_num"));

                    if(! row.isNull("app_top1_name"))
                        System.out.print("\t app_top1_name: " + row.getString("app_top1_name"));

                    if(! row.isNull("app_top2_name"))
                        System.out.print("\t app_top2_name: " + row.getString("app_top2_name"));

                    if(! row.isNull("app_top3_name"))
                        System.out.print("\t app_top3_name: " + row.getString("app_top3_name"));

                    if(! row.isNull("app_top1_count"))
                        System.out.print("\t app_top1_count: " + row.getString("app_top1_count"));

                    if(! row.isNull("app_top1_count"))
                        System.out.print("\t app_top2_count: " + row.getString("app_top2_count"));

                    if(! row.isNull("app_top3_count"))
                        System.out.print("\t app_top3_count: " + row.getString("app_top3_count"));

                    if(! row.isNull("update_dt"))
                        System.out.print("\t update_dt: " + row.getTimestamp("update_dt"));

                    if(! row.isNull("create_dt"))
                        System.out.print("\t create_dt: " + row.getTimestamp("create_dt"));

                    if(! row.isNull("flag_del"))
                        System.out.print("\t flag_del: " + row.getShort("flag_del"));

                    System.out.println();
                }
            }
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void CreateTable2(){
        this.kuduTableCol.setTableName("impala::sinaif_label.b_customer_action_product");

        List cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("customer_pk_id", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("op_time", Type.UNIXTIME_MICROS).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("action_code", Type.INT32).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("product_code_hash", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("product_code", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("action", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("terminal_code", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("channel_id", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("amount", Type.DOUBLE).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("flag_del", Type.INT16).defaultValue(Short.valueOf("0")).build());

        this.kuduTableCol.setTableSchema(cols);

        List rangeKeys = new ArrayList();
        rangeKeys.add("customer_pk_id");
        this.kuduTableCol.setTableBuilder(rangeKeys);

        try{
            if(! KUDU_CLIENT.tableExists(kuduTableCol.getTableName())){
                KuduTable table = KUDU_CLIENT.createTable(kuduTableCol.getTableName(), kuduTableCol.getTableSchema(), new CreateTableOptions().setRangePartitionColumns(kuduTableCol.getTableBuilder()));
            }
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public void ScanRow2(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("impala::sinaif_label.b_customer_action_product");
            KuduScanner scanner = KUDU_CLIENT.newScannerBuilder(table).build();
            int sum = 0;
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    sum += 1;
                    System.out.print(String.format("b_customer_action_product: 第 %d 条：", sum));

                    System.out.print("customer_pk_id: " + row.getLong("customer_pk_id"));

                    if(! row.isNull("op_time"))
                        System.out.print("\t op_time: " + row.getTimestamp("op_time"));

                    if(! row.isNull("product_code_hash"))
                        System.out.print("\t product_code_hash: " + row.getLong("product_code_hash"));

                    if(! row.isNull("action_code"))
                        System.out.print("\t action_code: " + row.getInt("action_code"));

                    if(! row.isNull("product_code"))
                        System.out.print("\t product_code: " + row.getString("product_code"));

                    if(! row.isNull("action"))
                        System.out.print("\t action: " + row.getString("action"));


                    if(! row.isNull("terminal_code"))
                        System.out.print("\t terminal_code: " + row.getString("terminal_code"));

                    if(! row.isNull("channel_id"))
                        System.out.print("\t channel_id: " + row.getString("channel_id"));

                    if(! row.isNull("amount"))
                        System.out.print("\t amount: " + row.getDouble("amount"));


                    if(! row.isNull("update_dt"))
                        System.out.print("\t update_dt: " + row.getTimestamp("update_dt"));

                    if(! row.isNull("create_dt"))
                        System.out.print("\t create_dt: " + row.getTimestamp("create_dt"));

                    if(! row.isNull("flag_del"))
                        System.out.print("\t flag_del: " + row.getShort("flag_del"));

                    System.out.println();
                }
            }
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void CreateTable3(){
        this.kuduTableCol.setTableName("impala::sinaif_label.b_customer_product_registe");

        List cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("customer_pk_id", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("terminal_code", Type.STRING).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("terminal_customer_id", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("register_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_time", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("flag_del", Type.INT16).defaultValue(Short.valueOf("0")).build());

        this.kuduTableCol.setTableSchema(cols);

        List rangeKeys = new ArrayList();
        rangeKeys.add("customer_pk_id");
        this.kuduTableCol.setTableBuilder(rangeKeys);

        try{
            if(! KUDU_CLIENT.tableExists(kuduTableCol.getTableName())){
                KuduTable table = KUDU_CLIENT.createTable(kuduTableCol.getTableName(), kuduTableCol.getTableSchema(), new CreateTableOptions().setRangePartitionColumns(kuduTableCol.getTableBuilder()));
            }
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public void ScanRow3(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("impala::sinaif_label.b_customer_product_registe");
            KuduScanner scanner = KUDU_CLIENT.newScannerBuilder(table).build();
            int sum = 0;
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    sum += 1;
                    System.out.print(String.format("b_customer_product_registe: 第 %d 条：", sum));

                    System.out.print("customer_pk_id: " + row.getLong("customer_pk_id"));

                    if(! row.isNull("terminal_code"))
                        System.out.print("\t terminal_code: " + row.getString("terminal_code"));

                    if(! row.isNull("terminal_customer_id"))
                        System.out.print("\t terminal_customer_id: " + row.getString("terminal_customer_id"));

                    if(! row.isNull("register_dt"))
                        System.out.print("\t register_dt: " + row.getTimestamp("register_dt"));

                    if(! row.isNull("create_time"))
                        System.out.print("\t create_time: " + row.getTimestamp("create_time"));

                    if(! row.isNull("update_dt"))
                        System.out.print("\t update_dt: " + row.getTimestamp("update_dt"));

                    if(! row.isNull("create_dt"))
                        System.out.print("\t create_dt: " + row.getTimestamp("create_dt"));

                    if(! row.isNull("flag_del"))
                        System.out.print("\t flag_del: " + row.getShort("flag_del"));

                    System.out.println();
                }
            }
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void CreateTable4(){
        this.kuduTableCol.setTableName("impala::sinaif_label.l_customer_finance_labels");

        List cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("customer_pk_id", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("bank_code", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("bank_has_credit", Type.BOOL).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("bank_credit_code", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("has_social_insurance", Type.BOOL).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("create_dt", Type.UNIXTIME_MICROS).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("flag_del", Type.INT16).defaultValue(Short.valueOf("0")).build());

        this.kuduTableCol.setTableSchema(cols);

        List rangeKeys = new ArrayList();
        rangeKeys.add("customer_pk_id");
        this.kuduTableCol.setTableBuilder(rangeKeys);

        try{
            if(! KUDU_CLIENT.tableExists(kuduTableCol.getTableName())){
                KuduTable table = KUDU_CLIENT.createTable(kuduTableCol.getTableName(), kuduTableCol.getTableSchema(), new CreateTableOptions().setRangePartitionColumns(kuduTableCol.getTableBuilder()));
            }
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public void ScanRow4(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("impala::sinaif_label.l_customer_finance_labels");
            KuduScanner scanner = KUDU_CLIENT.newScannerBuilder(table).build();
            int sum = 0;
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    sum += 1;
                    System.out.print(String.format("l_customer_finance_labels: 第 %d 条：", sum));

                    System.out.print("customer_pk_id: " + row.getLong("customer_pk_id"));

                    if(! row.isNull("bank_code"))
                        System.out.print("\t bank_code: " + row.getString("bank_code"));

                    if(! row.isNull("bank_has_credit"))
                        System.out.print("\t bank_has_credit: " + row.getBoolean("bank_has_credit"));

                    if(! row.isNull("bank_credit_code"))
                        System.out.print("\t bank_credit_code: " + row.getString("bank_credit_code"));

                    if(! row.isNull("has_social_insurance"))
                        System.out.print("\t has_social_insurance: " + row.getBoolean("has_social_insurance"));

                    if(! row.isNull("update_dt"))
                        System.out.print("\t update_dt: " + row.getTimestamp("update_dt"));

                    if(! row.isNull("create_dt"))
                        System.out.print("\t create_dt: " + row.getTimestamp("create_dt"));

                    if(! row.isNull("flag_del"))
                        System.out.print("\t flag_del: " + row.getShort("flag_del"));

                    System.out.println();
                }
            }
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void CreateTable5(){
        this.kuduTableCol.setTableName("impala:sinaif_label.b_customer_updates");

        List cols = new ArrayList<>();
        cols.add(new ColumnSchema.ColumnSchemaBuilder("customer_pk_id", Type.INT64).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_time", Type.UNIXTIME_MICROS).key(true).nullable(false).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("table_name_hash", Type.INT64).key(true).nullable(false).build());

        cols.add(new ColumnSchema.ColumnSchemaBuilder("terminal_customer_id", Type.INT64).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("terminal_code", Type.INT64).nullable(true).build());

        cols.add(new ColumnSchema.ColumnSchemaBuilder("database", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("table_name", Type.STRING).nullable(true).build());

        cols.add(new ColumnSchema.ColumnSchemaBuilder("main_change", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("change_content", Type.STRING).nullable(true).build());

        cols.add(new ColumnSchema.ColumnSchemaBuilder("operation", Type.STRING).nullable(true).build());
        cols.add(new ColumnSchema.ColumnSchemaBuilder("update_dt", Type.UNIXTIME_MICROS).nullable(true).build());

        this.kuduTableCol.setTableSchema(cols);

        List rangeKeys = new ArrayList();
        rangeKeys.add("customer_pk_id");
        this.kuduTableCol.setTableBuilder(rangeKeys);

        try{
            if(! KUDU_CLIENT.tableExists(kuduTableCol.getTableName())){
                KuduTable table = KUDU_CLIENT.createTable(kuduTableCol.getTableName(), kuduTableCol.getTableSchema(), new CreateTableOptions().setRangePartitionColumns(kuduTableCol.getTableBuilder()));
            }
        }catch (KuduException e){
            e.printStackTrace();
        }
    }

    public void ScanRow5(){
        try {
            KuduTable table = KUDU_CLIENT.openTable("impala:sinaif_label.b_customer_updates");
            KuduScanner scanner = KUDU_CLIENT.newScannerBuilder(table).build();
            int sum = 0;
            while (scanner.hasMoreRows()) {
                for (RowResult row : scanner.nextRows()) {
                    sum += 1;
                    System.out.print(String.format("b_customer_updates: 第 %d 条：", sum));

                    System.out.print("customer_pk_id: " + row.getLong("customer_pk_id"));

                    if(! row.isNull("update_time"))
                        System.out.print("\t update_time: " + row.getTimestamp("update_time"));

                    if(! row.isNull("table_name_hash"))
                        System.out.print("\t table_name_hash: " + row.getLong("table_name_hash"));

                    if(! row.isNull("terminal_customer_id"))
                        System.out.print("\t terminal_customer_id: " + row.getLong("terminal_customer_id"));

                    if(! row.isNull("terminal_code"))
                        System.out.print("\t terminal_code: " + row.getLong("terminal_code"));

                    if(! row.isNull("main_change"))
                        System.out.print("\t main_change: " + row.getString("main_change"));

                    if(! row.isNull("operation"))
                        System.out.print("\t operation: " + row.getString("operation"));

                    if(! row.isNull("change_content"))
                        System.out.print("\t change_content: " + row.getString("change_content"));

                    if(! row.isNull("database"))
                        System.out.print("\t database: " + row.getString("database"));

                    if(! row.isNull("table_name"))
                        System.out.print("\t table_name: " + row.getString("table_name"));

                    if(! row.isNull("update_dt"))
                        System.out.print("\t update_dt: " + row.getTimestamp("update_dt"));

                    System.out.println();

                }
            }
        }catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
