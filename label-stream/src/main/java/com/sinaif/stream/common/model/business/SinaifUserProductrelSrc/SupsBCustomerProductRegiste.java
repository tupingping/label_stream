package com.sinaif.stream.common.model.business.SinaifUserProductrelSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduDomain;

import java.util.Date;


/**
 * @Time : 2019/8/29 16:56
 * @Author : pingping.tu
 * @File : SupsBCustomerProductRegiste.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.b_customer_product_registe")
public class SupsBCustomerProductRegiste extends CommonBusiness {

    @KuduColumn(property = "terminal_code")
    public String productid;

    @KuduColumn(property = "terminal_customer_id")
    public String accountid;

    @KuduColumn(property = "register_dt")
    public Date regtime;

    @Override
    public Boolean filter() {
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SupsBCustomerActionProductBase supsBCustomerActionProductBase = (SupsBCustomerActionProductBase)src;
        this.log = supsBCustomerActionProductBase.log;
        flag_del = supsBCustomerActionProductBase.flag_del;
        customerId = supsBCustomerActionProductBase.customerId;
        productid = supsBCustomerActionProductBase.productid;
        accountid = supsBCustomerActionProductBase.accountid;
        regtime = supsBCustomerActionProductBase.regtime;
    }

}
