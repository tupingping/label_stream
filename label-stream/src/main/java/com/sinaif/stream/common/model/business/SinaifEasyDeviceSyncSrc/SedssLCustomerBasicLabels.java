package com.sinaif.stream.common.model.business.SinaifEasyDeviceSyncSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;


/**
 * @Time : 2019/8/23 14:01
 * @Author : pingping.tu
 * @File : SedssLCustomerBasicLabels.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class SedssLCustomerBasicLabels extends CommonBusiness {

    @KuduColumn(property = "ua_phone_num")
    public String phone;


    @Override
    public Boolean filter() {
        if(phone == null){
            return false;
        }
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SedssBCustomerInfoBase sedssBCustomerInfoBase = (SedssBCustomerInfoBase)src;
        this.log = sedssBCustomerInfoBase.log;
        flag_del = sedssBCustomerInfoBase.flag_del;
        customerId = sedssBCustomerInfoBase.customerId;
        phone = sedssBCustomerInfoBase.phone;
    }
}
