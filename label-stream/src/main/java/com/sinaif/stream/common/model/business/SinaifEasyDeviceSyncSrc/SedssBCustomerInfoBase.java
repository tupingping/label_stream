package com.sinaif.stream.common.model.business.SinaifEasyDeviceSyncSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.model.business.ModifyHistory;
import com.sinaif.stream.common.utils.AESToLong;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;
import com.sinaif.stream.kudu.connector.KuduEventSaver;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Time : 2019/8/21 12:08
 * @Author : pingping.tu
 * @File : SccsBCustomerInfoBase.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_info")
public class SedssBCustomerInfoBase extends CommonBusiness {

    @KuduColumn(property = "phone_num")
    public String phone;

    @KuduColumnIgnore
    public Date createtime;

    @Override
    public Boolean filter() {
        if(phone == null){
            return false;
        }
        return super.filter();
    }

    public void prepare(Object src){
        super.TypeConventer();

        customerId = AESToLong.convertToNumber(phone);
    }

    @Override
    public List<Tuple2<Object, KuduEventSaver.WriteMode>> convert() {
        List<Tuple2<Object, KuduEventSaver.WriteMode>> result = new ArrayList<>();

        // TODO: add data objects

        // b_customer_info
        result.add(this.get(this));

        // l_customer_basic_labels
        SedssLCustomerBasicLabels sedssLCustomerBasicLabels = new SedssLCustomerBasicLabels();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp = sedssLCustomerBasicLabels.get(this);
        if(((SedssLCustomerBasicLabels)tmp.f0).filter()){
            result.add(tmp);
        }

        // b_customer_action_product
        SedssBCustomerActionProduct sedssBCustomerActionProduct = new SedssBCustomerActionProduct();
        Tuple2<Object, KuduEventSaver.WriteMode> tmp1 = sedssBCustomerActionProduct.get(this);
        if(((SedssBCustomerActionProduct)tmp1.f0).filter()){
            result.add(tmp1);
        }

        if(enableHistory && rawType.toLowerCase().equals("update")){
            ModifyHistory history = ModifyHistory.generate(log,rawType);
            result.add(new Tuple2<>(history, KuduEventSaver.WriteMode.INSERT));
        }

        return result;
    }
}
