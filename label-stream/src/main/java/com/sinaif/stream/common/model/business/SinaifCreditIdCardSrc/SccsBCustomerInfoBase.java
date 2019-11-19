package com.sinaif.stream.common.model.business.SinaifCreditIdCardSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.model.business.ModifyHistory;
import com.sinaif.stream.common.utils.AESToLong;
import com.sinaif.stream.common.utils.ConfigRespostory;
import com.sinaif.stream.common.utils.IdCardDecode;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import com.sinaif.stream.redis.RedisPool;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Time : 2019/8/20 17:18
 * @Author : pingping.tu
 * @File : SccsBCustomerInfoBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

@KuduDomain(table = "impala::stream_test.b_customer_info")
public class SccsBCustomerInfoBase extends CommonBusiness {

    @KuduColumn(property = "idc_card_name")
    public String cardname;

    @KuduColumn(property = "idc_cardno")
    public String cardno;

    public String phone_num;

    public Integer birthday;

    public Short sex;

    public String race;

    @KuduColumn(property = "create_dt")
    public Date createtime;

    @KuduColumnIgnore
    public String userid;

    @KuduColumnIgnore
    public String productid;

    @KuduColumnIgnore
    public String idc_cardno_code;

    @KuduColumnIgnore
    public String idc_zodiac;

    @KuduColumnIgnore
    public String idc_constellation;

    @KuduColumnIgnore
    public String famadr;

    @KuduColumnIgnore
    public Long validstartdate;

    @KuduColumnIgnore
    public Long validenddate;

    @KuduColumnIgnore
    public String agency;

    @Override
    public Boolean filter() {
        String field = userid + "@" + productid;
        String value = RedisPool.hget(ConfigRespostory.value("redis.cache.hash.key"), field);
        if(value == null || cardno == null){
            return false;
        }
        String[] values = value.split("@");
        customerId = Long.valueOf(values[1]);
        phone_num = values[0];

        return super.filter();
    }

    public void prepare(Object src){
        super.TypeConventer();

        String idcard = IdCardDecode.getIdCard(cardno, productid);
        cardno = AESToLong.encrypt(idcard);
        birthday = IdCardDecode.getBirthday(idcard);
        sex = IdCardDecode.getSex(idcard);
        idc_cardno_code = IdCardDecode.getAreaCode(idcard);
        idc_zodiac = IdCardDecode.getZodiac(idcard);
        idc_constellation = IdCardDecode.getChineseZodiac(idcard);
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_info
        result.add(this.get(this));

        // l_customer_basic_labels
        SccsLCustomerBasicLabels sccsLCustomerBasicLabels = new SccsLCustomerBasicLabels();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp = sccsLCustomerBasicLabels.get(this);
        if(((SccsLCustomerBasicLabels)tmp.f0).filter()){
            result.add(tmp);
        }

        if(enableHistory && rawType.toLowerCase().equals("update")){
            ModifyHistory history = ModifyHistory.generate(log, rawType);
            result.add(new Tuple2<>(history, KuduEventSaver.WriteMode.INSERT));
        }

        return result;
    }
}
