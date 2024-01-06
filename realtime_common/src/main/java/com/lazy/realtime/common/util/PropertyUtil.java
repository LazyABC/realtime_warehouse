package com.lazy.realtime.common.util;

import java.util.ResourceBundle;

/**
 * @Name: Lazy
 * @Date: 2023/12/28 18:50:07
 * @Details:
 */
public class PropertyUtil {
    //直接读取类路径下的配置文件，只需要传入配置文件的名字(不带后缀)。把ResourceBundle当Map用
    static ResourceBundle config = ResourceBundle.getBundle("config");

    //工具方法都是静态的
    public static String getStringValue(String name){
        //根据属性名从Map中获取属性值
        return config.getString(name);
    }

    public static Boolean getBooleanValue(String name){
        return Boolean.valueOf(config.getString(name));
    }

    public static Integer getIntValue(String name){
        return Integer.parseInt(config.getString(name));
    }

    public static void main(String[] args) {

        System.out.println(getStringValue("KAFKA_BROKERS"));
        System.out.println(getStringValue("MYSQL_PORT"));

    }
}
