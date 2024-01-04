package com.lazy.realtime.dwd.db.interaction;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 11:13:48
 * @Details:
 *  业务过程：  专注于评论。
 *  业务表:    comment_info
 *
 *  业务流程:  用户可以评论------>追评
 *           用户可以评论----->删除评论
 *           用户可以评论----->删除评论------>添加评论
 *
 *  目前只关心是用户添加评论这个业务过程。
 *     采集 comment_info的insert操作。
 *
 *  尽量补充丰富的维度：  把维度编码替换为维度的字段说明。
 *
 *  -------------------------
 *     事务事实表：
 *             需要什么字段，取决于后续的DWS,ADS层要统计什么字段。
 *             准备地描述这个事实:
 *                     3W(who,when,where) + 事实说明。
 *             需要将appraise，由编码丰富为具体的评论类型。
 *
 *  --------------------------
 *      关联 base_dic(存在在hbase,采用look-up join的方式)，获取编码的含义。
 *
 *  ------------------
 *     技术的选择:
 *             由于要使用look-up join，只能用flinksql。
 *             只有flinksql才提供了hbase连接器，提供了look-up join的语法。
 */
public class CommentInfo extends BaseSqlApp {
    public static void main(String[] args) {
        new CommentInfo()
                .start(
                        TOPIC_DWD_INTERACTION_COMMENT_INFO,
                        11002,
                        4
                );
    }
    @Override
    protected void handle(TableEnvironment env) {
        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDb(env);
        //2.过滤出 comment_info的insert操作的数据
        String commentInfoSql = "select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['spu_id'] spu_id, " +
                "  data['order_id'] order_id, " +
                "  data['appraise'] appraise, " +
                "  data['comment_txt'] comment_txt , " +
                "  data['create_time'] create_time ,  " +
                "   ts ,  " +
                "   pt   " +
                " from ods_db " +
                " where `database` = 'gmall'" +
                " and `table` = 'comment_info'" +
                " and `type` = 'insert' ";
        env.createTemporaryView("commentInfo",env.sqlQuery(commentInfoSql));

        //3.创建一个look-up表，映射hbase中的   dim_dic_code
        createDimBaseDic(env);

        //4.进行look-upjoin
        //要求事实表中必须有处理时间
        String joinSql =" SELECT commentInfo.id,user_id,sku_id,spu_id,order_id,appraise , dim_dic_code.info.dic_name," +
                "  comment_txt, create_time, ts " +
                " FROM commentInfo  " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF commentInfo.pt " +
                "    ON commentInfo.appraise = dim_dic_code.id; ";



        //5.创建一个sink表，把需要的字段写出到kafka
        String sinkSql = " create table " + TOPIC_DWD_INTERACTION_COMMENT_INFO +"(" +
                "   id STRING, " +
                "   user_id STRING, " +
                "   sku_id STRING, " +
                "   spu_id  STRING, " +
                "   order_id STRING, " +
                "   appraise STRING, " +
                "   appraiseName STRING, " +
                "   comment_txt STRING, " +
                "   create_time STRING,  " +
                "   ts BIGINT " + SqlUtil.getKafkaSinkSql(TOPIC_DWD_INTERACTION_COMMENT_INFO);

        env.executeSql(sinkSql);

        //6.执行写出
        env.executeSql("insert into " + TOPIC_DWD_INTERACTION_COMMENT_INFO +  joinSql);
    }
}
