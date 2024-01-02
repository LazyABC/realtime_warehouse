package com.lazy.realtime.common.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Smexy on 2024/1/2

    常用的工具类：
        apache.commons提供的
        guava:  google java
 */
public class JDBCUtil
{
    //准备一个连接池
    private static DruidDataSource dataSource;

    static {
        //连接池的参数，参数config.properties

            // 初始化连接池配置
            dataSource = new DruidDataSource();
            dataSource.setUrl(PropertyUtil.getStringValue("MYSQL_URL"));
            dataSource.setUsername(PropertyUtil.getStringValue("MYSQL_USER"));
            dataSource.setPassword(PropertyUtil.getStringValue("MYSQL_PASSWORD"));

            // 其他连接池配置...
            dataSource.setInitialSize(PropertyUtil.getIntValue("POOL_INITIAL_SIZE")); // 初始化连接数
            dataSource.setMaxActive(PropertyUtil.getIntValue("POOL_MAX_ACTIVE")); // 最大连接数
            dataSource.setMinIdle(PropertyUtil.getIntValue("POOL_MIN_IDLE")); // 最小空闲连接数
            dataSource.setMaxWait(PropertyUtil.getIntValue("POOL_MAX_WAITTIME")); // 获取连接的最大等待时间，单位毫秒

    }

    //可以从池子中获取连接
    public static Connection getConn() throws SQLException {
        return dataSource.getConnection();
    }

    //把连接还入池子
    public static void closeConn(Connection conn) throws SQLException {
        if (conn != null){
            conn.close();
        }
    }

    //提供可以根据一条sql语句，查询一个List<T>的方法
    public static <T> List<T> queryBeanList(String sql,Class<T> t) throws SQLException {

        //1.创建查询的入口
        QueryRunner queryRunner = new QueryRunner();

        //2.查询
        Connection conn = getConn();
        BeanListHandler<T> handler = new BeanListHandler<>(t, new BasicRowProcessor(new GenerousBeanProcessor()));
        List<T> result = queryRunner.query(conn, sql, handler);

        //3.归还连接
        closeConn(conn);
        return result;
    }

}
