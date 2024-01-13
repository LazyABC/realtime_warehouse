package com.lazy.realtime.ads.util;

import org.springframework.util.StringUtils;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 08:57:25
 * @Details:
 */
public class DataUtil {

    //校验日期参数。如果传入的日期字符串为空，它将使用当前日期（使用LocalDate.now()获取）格式化为"yyyyMMdd"的形式
    public static String validDate(String date){
        if (!StringUtils.hasText(date)){
            date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
        return date;
    }


    static final DecimalFormat df = new DecimalFormat("#.00");


    //格式化Double类型的数据，保留两位小数,它使用了DecimalFormat类，将传入的Double数值格式化为保留两位小数的形式，并返回结果。
    public static Double validDouble(Double number){
        return  Double.parseDouble(df.format(number));
    }
}
