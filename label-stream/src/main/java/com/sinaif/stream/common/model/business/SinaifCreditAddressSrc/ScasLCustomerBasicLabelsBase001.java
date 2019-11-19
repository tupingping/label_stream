package com.sinaif.stream.common.model.business.SinaifCreditAddressSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.kudu.connector.KuduColumn;
import com.sinaif.stream.kudu.connector.KuduColumnIgnore;
import com.sinaif.stream.kudu.connector.KuduDomain;

/**
 * @Time : 2019/8/26 16:18
 * @Author : pingping.tu
 * @File : ScasLCustomerBasicLabelsBase001.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class ScasLCustomerBasicLabelsBase001 extends CommonBusiness {

    @KuduColumn(property = "credit_company_province")
    public String addrprovince;

    @KuduColumn(property = "credit_company_city")
    public String addrcity;

    @KuduColumn(property = "credit_company_area")
    public String addrarea;

    @KuduColumn(property = "credit_company_addr")
    public String fulladdr;

    @KuduColumn(property = "credit_company_name")
    public String companyname;

    @KuduColumn(property = "credit_company_zone")
    public String companyzone;

    @KuduColumn(property = "credit_company_telephone")
    public String comptelephone;

    @KuduColumn(property = "credit_comp_extension_num")
    public String compextensionnum;

    @KuduColumnIgnore
    public Integer addrtype;

    @KuduColumnIgnore
    public String productid;

    @KuduColumnIgnore
    public String id;


    @Override
    public Boolean filter() {
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        ScasLCustomerBasicLabelsBase scasLCustomerBasicLabelsBase = (ScasLCustomerBasicLabelsBase)src;
        this.log = scasLCustomerBasicLabelsBase.log;
        flag_del = scasLCustomerBasicLabelsBase.flag_del;

        customerId = scasLCustomerBasicLabelsBase.customerId;
        addrprovince = scasLCustomerBasicLabelsBase.addrprovince;
        addrcity = scasLCustomerBasicLabelsBase.addrcity;
        addrarea = scasLCustomerBasicLabelsBase.addrarea;
        fulladdr = scasLCustomerBasicLabelsBase.fulladdr;
        companyname = scasLCustomerBasicLabelsBase.companyname;
        companyzone = scasLCustomerBasicLabelsBase.companyzone;
        comptelephone = scasLCustomerBasicLabelsBase.comptelephone;
        compextensionnum = scasLCustomerBasicLabelsBase.compextensionnum;

    }

}
