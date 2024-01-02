package com.lazy.realtime.common.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @Name: Lazy
 * @Date: 2024/1/2 18:09:55
 * @Details:
 */
public class DateTimeFormatUtil {
    //定义时间格式化的样式
    private static DateTimeFormatter dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // 把 毫秒的 ts 转换为 日期+时间
    public static String parseTsToDateTime(long ts){

        return LocalDateTime
                .ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault())
                .format(dateTimeFormat);

    }

    // 把 毫秒的 ts 转换为 日期
    public static String parseTsToDate(long ts){

        return LocalDateTime
                .ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault())
                .format(dateFormat);

    }

    // 把  日期  转换为 毫秒的 ts
    public static long parseDateToTs(String dateStr){

        return LocalDate
                .parse(dateStr,dateFormat)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();

    }

    // 把 日期+时间  转换为 毫秒的 ts
    public static long parseDateTimeToTs(String dateTimeStr){

        return LocalDateTime
                .parse(dateTimeStr,dateTimeFormat)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();

    }

    public static void main(String[] args) {

        System.out.println(parseDateTimeToTs("2024-01-02 15:47:00"));

        System.out.println(parseDateToTs("2024-01-02"));

        System.out.println(parseTsToDate(1704124800000l));

        System.out.println(parseTsToDateTime(1704124800000l));

    }
}
