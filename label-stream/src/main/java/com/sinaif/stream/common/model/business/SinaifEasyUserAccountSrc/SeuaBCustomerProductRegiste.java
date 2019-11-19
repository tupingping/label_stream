package com.sinaif.stream.common.model.business.SinaifEasyUserAccountSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;

/**
 * @Time : 2019/8/29 17:21
 * @Author : pingping.tu
 * @File : SeuaBCustomerProductRegiste.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_product_registe")
public class SeuaBCustomerProductRegiste extends CommonBusiness {

    public String terminal_code = "1003";

    @KuduColumn(property = "terminal_customer_id")
    public String id;

    @KuduColumn(property = "register_dt")
    public Date registtime;

    @Override
    public Boolean filter() {
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SeuasBCustomerInfoBase seuasBCustomerInfoBase = (SeuasBCustomerInfoBase)src;
        this.log = seuasBCustomerInfoBase.log;

        flag_del = seuasBCustomerInfoBase.flag_del;
        customerId = seuasBCustomerInfoBase.customerId;
        registtime = seuasBCustomerInfoBase.registtime;
    }
}
