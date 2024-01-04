package com.lazy.realtime.dwd.db.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_CART_ADD;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 15:11:41
 * @Details:
 * 业务过程:  加购。
 *     首次向购物车中添加商品: 向cart_info表insert一条记录。
 *     增加购物车中商品的数量: 向cart_info表update一条记录，update后sku_num必须大于更新前
 */
public class CartAdd extends BaseSqlApp {
    public static void main(String[] args) {

        new CartAdd()
                .start(
                        TOPIC_DWD_TRADE_CART_ADD,
                        11002,
                        4
                );

    }
    /*
        IFNULL(input, null_replacement): 是flink中用于判断NULL赋予默认值的函数。
                类似hive中的nvl();
                    input 和 null_replacement的类型必须相同
     */
    @Override
    protected void handle(TableEnvironment env) {
        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDb(env);
        //2.过滤出 cart_info的insert和update操作的数据
        String cartInfoSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['cart_price'] cart_price, " +
                "  data['sku_name'] sku_name, " +
                "  cast(data['sku_num'] as int) - cast(ifnull(`old`['sku_num'],'0') as int)  sku_num, " +
                "   ts   " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'cart_info' " +
                " and (`type` = 'insert' or (`type` = 'update' and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int) )) ";


        //3.创建一个sink表
        String sinkSql = " create table " + TOPIC_DWD_TRADE_CART_ADD +"(" +
                " id STRING, " +
                " user_id STRING, " +
                " sku_id STRING, " +
                " cart_price STRING, " +
                " sku_name STRING , " +
                "  sku_num INT , " +
                "   ts BIGINT " + SqlUtil.getKafkaSinkSql(TOPIC_DWD_TRADE_CART_ADD);

        env.executeSql(sinkSql);

        //4.写出
        env.executeSql("insert into "+TOPIC_DWD_TRADE_CART_ADD + cartInfoSql);
    }
}
