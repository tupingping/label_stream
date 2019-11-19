package com.sinaif.stream.common.utils;

/**
 * @Time : 2019/8/26 9:39
 * @Author : pingping.tu
 * @File : PhoneNumberGeo.py
 * @Email : flatuer@gmail.com
 * @Description :
 */

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class PhoneNumberGeo {
    private static String[] numberType = new String[]{null, "移动", "联通", "电信", "电信虚拟运营商", "联通虚拟运营商", "移动虚拟运营商"};
    private static final int INDEX_SEGMENT_LENGTH = 9;
    private static byte[] dataByteArray;
    private ByteBuffer byteBuffer;
    private int indexAreaOffset = -1;
    private int phoneRecordCount = -1;

    public PhoneNumberGeo() {
        if (dataByteArray == null) {
            Class var1 = PhoneNumberGeo.class;
            synchronized(PhoneNumberGeo.class) {
                if (dataByteArray == null) {
                    ByteArrayOutputStream byteData = new ByteArrayOutputStream();
                    byte[] buffer = new byte[1024];

                    try {
                        InputStream inputStream = PhoneNumberGeo.class.getClassLoader().getResourceAsStream("phone.dat");

                        while(true) {
                            int readBytesLength;
                            if ((readBytesLength = inputStream.read(buffer)) == -1) {
                                inputStream.close();
                                break;
                            }

                            byteData.write(buffer, 0, readBytesLength);
                        }
                    } catch (Exception var7) {
                        System.err.println("Can't find phone.dat in classpath: phone.dat");
                        var7.printStackTrace();
                        throw new RuntimeException(var7);
                    }

                    dataByteArray = byteData.toByteArray();
                }
            }
        }

        this.byteBuffer = ByteBuffer.wrap(dataByteArray);
        this.byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int dataVersion = this.byteBuffer.getInt();
        this.indexAreaOffset = this.byteBuffer.getInt();
        this.phoneRecordCount = (dataByteArray.length - this.indexAreaOffset) / 9;
    }

    public PhoneNumberInfo lookup(String phoneNumber) {
        if (phoneNumber != null && phoneNumber.length() <= 11 && phoneNumber.length() >= 7) {
            int phoneNumberPrefix;
            try {
                phoneNumberPrefix = Integer.parseInt(phoneNumber.substring(0, 7));
            } catch (Exception var14) {
                return null;
            }

            int left = 0;
            int right = this.phoneRecordCount;

            while(left <= right) {
                int middle = left + right >> 1;
                int currentOffset = this.indexAreaOffset + middle * 9;
                if (currentOffset >= dataByteArray.length) {
                    return null;
                }

                this.byteBuffer.position(currentOffset);
                int currentPrefix = this.byteBuffer.getInt();
                if (currentPrefix > phoneNumberPrefix) {
                    right = middle - 1;
                } else {
                    if (currentPrefix >= phoneNumberPrefix) {
                        int infoBeginOffset = this.byteBuffer.getInt();
                        int phoneType = this.byteBuffer.get();
                        int infoLength = -1;

                        for(int i = infoBeginOffset; i < this.indexAreaOffset; ++i) {
                            if (dataByteArray[i] == 0) {
                                infoLength = i - infoBeginOffset;
                                break;
                            }
                        }

                        String infoString = new String(dataByteArray, infoBeginOffset, infoLength, StandardCharsets.UTF_8);
                        String[] infoSegments = infoString.split("\\|");
                        PhoneNumberInfo phoneNumberInfo = new PhoneNumberInfo();
                        phoneNumberInfo.setPhoneNumber(phoneNumber);
                        phoneNumberInfo.setProvince(infoSegments[0]);
                        phoneNumberInfo.setCity(infoSegments[1]);
                        phoneNumberInfo.setZipCode(infoSegments[2]);
                        phoneNumberInfo.setAreaCode(infoSegments[3]);
                        phoneNumberInfo.setPhoneType(numberType[phoneType]);
                        return phoneNumberInfo;
                    }

                    left = middle + 1;
                }
            }

            return null;
        } else {
            return null;
        }
    }

    public static void main(String[] args) {
        PhoneNumberGeo phoneNumberGeo = new PhoneNumberGeo();
        PhoneNumberInfo info = phoneNumberGeo.lookup("18320838131");
        System.out.println(info.getProvince());
        System.out.println(info.getCity());
        System.out.println(info.getAreaCode());
    }
}
