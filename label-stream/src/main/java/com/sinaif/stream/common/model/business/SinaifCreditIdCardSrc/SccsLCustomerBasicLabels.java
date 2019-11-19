package com.sinaif.stream.common.model.business.SinaifCreditIdCardSrc;

import com.sinaif.stream.common.model.business.CommonBusiness;
import com.sinaif.stream.common.utils.PhoneNumberGeo;
import com.sinaif.stream.common.utils.PhoneNumberInfo;
import com.sinaif.stream.kudu.connector.KuduDomain;


/**
 * @Time : 2019/8/23 14:46
 * @Author : pingping.tu
 * @File : SccsLCustomerBasicLabels.py
 * @Email : flatuer@gmail.com
 * @Description :
 */
@KuduDomain(table = "impala::stream_test.l_customer_basic_labels")
public class SccsLCustomerBasicLabels extends CommonBusiness {

    public String idc_cardno_code;


    public String idc_zodiac;


    public Integer idc_birthday;


    public String idc_constellation;


    public Short idc_sex;


    public String ua_phone_province;


    public String ua_phone_city;


    public String ua_phone_carrier;


    public String ua_phone_areacode;


    public String ua_phone_zipcode;


    public String ua_phone_num;


    public String idc_cardno;


    public String idc_card_name;


    public String idc_race;


    public String idc_card_address;


    public Long idc_card_valid_start_date;


    public Long idc_card_valid_end_date;


    public String idc_agency;


    @Override
    public Boolean filter() {
        return super.filter();
    }

    @Override
    public void prepare(Object src) {
        SccsBCustomerInfoBase sccsBCustomerInfoBase = (SccsBCustomerInfoBase)src;
        this.log = sccsBCustomerInfoBase.log;

        flag_del = sccsBCustomerInfoBase.flag_del;
        customerId = sccsBCustomerInfoBase.customerId;
        idc_cardno = sccsBCustomerInfoBase.cardno;
        idc_card_name = sccsBCustomerInfoBase.cardname;
        idc_race = sccsBCustomerInfoBase.race;
        idc_cardno_code = sccsBCustomerInfoBase.idc_cardno_code;
        idc_zodiac = sccsBCustomerInfoBase.idc_zodiac;
        idc_birthday = sccsBCustomerInfoBase.birthday;
        idc_constellation = sccsBCustomerInfoBase.idc_constellation;
        idc_sex = sccsBCustomerInfoBase.sex;
        idc_card_address = sccsBCustomerInfoBase.famadr;
        idc_card_valid_start_date = sccsBCustomerInfoBase.validstartdate;
        idc_card_valid_end_date = sccsBCustomerInfoBase.validenddate;
        idc_agency = sccsBCustomerInfoBase.agency;

        ua_phone_num = sccsBCustomerInfoBase.phone_num;
        PhoneNumberGeo phoneNumberGeo = new PhoneNumberGeo(); // not a good way
        PhoneNumberInfo info = phoneNumberGeo.lookup(ua_phone_num);

        ua_phone_province = info.getProvince();
        ua_phone_city = info.getCity();
        ua_phone_carrier = info.getPhoneType();
        ua_phone_areacode = info.getAreaCode();
        ua_phone_zipcode = info.getZipCode();

    }
}
