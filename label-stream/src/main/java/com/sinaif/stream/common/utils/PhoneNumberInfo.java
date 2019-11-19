package com.sinaif.stream.common.utils;

/**
 * @Time : 2019/8/26 9:39
 * @Author : pingping.tu
 * @File : PhoneNumberInfo.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

public class PhoneNumberInfo {
    private String phoneNumber;
    private String province;
    private String city;
    private String zipCode;
    private String areaCode;
    private String phoneType;

    public PhoneNumberInfo() {
    }

    public String getPhoneNumber() {
        return this.phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getProvince() {
        return this.province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return this.city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getZipCode() {
        return this.zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getAreaCode() {
        return this.areaCode;
    }

    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }

    public String getPhoneType() {
        return this.phoneType;
    }

    public void setPhoneType(String phoneType) {
        this.phoneType = phoneType;
    }

    public String toString() {
        return "PhoneNumberInfo{phoneNumber='" + this.phoneNumber + '\'' + ", province='" + this.province + '\'' + ", city='" + this.city + '\'' + ", zipCode='" + this.zipCode + '\'' + ", areaCode='" + this.areaCode + '\'' + ", phoneType='" + this.phoneType + '\'' + '}';
    }
}
