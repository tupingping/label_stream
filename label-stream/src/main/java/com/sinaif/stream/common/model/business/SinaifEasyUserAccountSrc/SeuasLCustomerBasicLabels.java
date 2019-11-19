package com.sinaif.stream.common.model.business.SinaifEasyUserAccountSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;


/**
 * @Time : 2019/8/23 14:17
 * @Author : pingping.tu
 * @File : SeuasLCustomerBasicLabels.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class SeuasLCustomerBasicLabels extends CommonBusiness {

    @KuduColumn(property = "ua_phone_num")
    public String username;

    @Override
    public Boolean filter() {
        if(username == null){
            return false;
        }
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SeuasBCustomerInfoBase seuasBCustomerInfoBase = (SeuasBCustomerInfoBase)src;
        this.log = seuasBCustomerInfoBase.log;

        flag_del = seuasBCustomerInfoBase.flag_del;
        customerId = seuasBCustomerInfoBase.customerId;
        username = seuasBCustomerInfoBase.username;
    }

}
