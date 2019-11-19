package com.sinaif.stream.common.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * TODO need to fix lately
 * @author simonzhang
 *
 */
public class AESToLong {

	private static Logger LOGGER = LoggerFactory.getLogger(AESToLong.class);

	/**
     * 加密用的Key 可以用26个字母和数字组成
     * 此处使用AES-128-CBC加密模式，key需要为16位。
     */
	private static String SECURITY_KEY = "sinaiftagssinaif";
    private static String ivParameter = "sinaiflabesinaif";
 
    /**
     *  save memory space
     */
	private static Cipher CLIPHER;
	static {
		try {
			CLIPHER = Cipher.getInstance("AES/CBC/PKCS5Padding");
			SecretKeySpec skeySpec = new SecretKeySpec(SECURITY_KEY.getBytes(), "AES");
			IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
			CLIPHER.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
		} catch (Exception e) {
			LOGGER.error(e.toString());
		}
	}

    // 加密
    public static String encrypt(String sSrc){
    	try {
    		 byte[] encrypted = CLIPHER.doFinal(sSrc.getBytes("utf-8"));
    		 return Base64.getEncoder().encodeToString(encrypted);//此处使用BASE64做转码。
    	} catch(Exception e) {
    		throw new RuntimeException(e);
    	}     
    }


    
	public static long convertToNumber(String sSrc) {
		byte[] encrypted = null;
		try {
			encrypted = CLIPHER.doFinal(sSrc.getBytes("utf-8"));
		}  catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		StringBuffer sb = new StringBuffer();
		for (int n = 0; n < encrypted.length; n++) {
			String strHex = Integer.toHexString(encrypted[n] & 0xFF);
			sb.append((strHex.length() == 1) ? "0" + strHex : strHex); // 每个字节由两个字符表示，位数不够，高位补0
		}
		
		int length = sb.capacity() > 16 ? 16 : sb.capacity();
		byte[] convertTmp = sb.subSequence(0, length).toString().getBytes();
		
//		System.out.println("hex:" + sb.toString()+ " " + length + ": " + sb.subSequence(0, length).toString());
		long result = 0L;
		for (int i = 0; i < convertTmp.length; i++) {
			//加下一位的字符时，先将前面字符计算的结果左移4位
			result <<= 4;
			//0-9数组
			byte b = (byte) (convertTmp[i] - 48);
			//A-F字母
			if (b > 9) {
				b = (byte) (b - 39);
			}
			//非16进制的字符
			if (b > 15 || b < 0) {
				throw new NumberFormatException("For input string '" + sSrc);
			}
			result += b;
		}
		return result & 0x7fffffffffffffffL;
	}
	
    public static void main(String[] args) {
        String phone = "13928437714";
        try {
            String sec = encrypt(phone);
            System.out.println(sec);
            System.out.println(convertToNumber(phone));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
         phone = "13928437715";
        try {
            String sec = encrypt(phone);
            System.out.println(sec);
            System.out.println(convertToNumber(phone));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        phone = "13928437716";
        try {
            String sec = encrypt(phone);
            System.out.println(sec);
            System.out.println(convertToNumber(phone));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
