package com.sinaif.stream.common.model.business.SinaifUserAccountSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;


/**
 * @Time : 2019/8/23 10:26
 * @Author : pingping.tu
 * @File : SuasLCustomerBasicLabels.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class SuasLCustomerBasicLabels extends CommonBusiness {

    @KuduColumn(property = "ua_phone_num")
    public String username;

    @Override
    public Boolean filter() {
        return super.filter();
    }


    @Override
    public void prepare(Object src) {
        SuasBCustomerInfoBase suasBCustomerInfoBase = (SuasBCustomerInfoBase)src;
        this.log = suasBCustomerInfoBase.log;

        flag_del = suasBCustomerInfoBase.flag_del;
        customerId = suasBCustomerInfoBase.customerId;
        username = suasBCustomerInfoBase.username;

    }

}
