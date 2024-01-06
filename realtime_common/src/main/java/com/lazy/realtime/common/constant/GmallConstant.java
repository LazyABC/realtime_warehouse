package com.lazy.realtime.common.constant;

public interface GmallConstant
{
    //接口中只能编写常量，变量的修饰符 是  static final
    //日志中的标记
    String START = "start";
    String PAGE = "page";
    String ACTIONS = "actions";
    String DISPLAYS = "displays";
    String ERR = "err";
    //新老客户的标记
    String ISNEWOLD = "0";
    String ISNEWNEW = "1";
    //dwd日志相关
    String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";

    String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    String TOPIC_DWD_TRADE_CANCEL_DETAIL = "dwd_trade_cancel_detail";
    String TOPIC_DWD_TRADE_PAY_DETAIL_SUC = "dwd_trade_pay_detail_suc";
    String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    String TOPIC_DWD_TRADE_REFUND_PAY_SUC = "dwd_trade_refund_pay_suc";
    String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    //dws相关
    String DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    String DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW = "dws_traffic_vc_ch_ar_is_new_page_view_window";
    String DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW = "dws_traffic_home_detail_page_view_window";
    String DWS_USER_USER_LOGIN_WINDOW = "dws_user_user_login_window";
    String DWS_USER_USER_REGISTER_WINDOW ="dws_user_user_register_window";
    String DWS_TRADE_CART_ADD_UU_WINDOW = "dws_trade_cart_add_uu_window";
    String DWS_TRADE_PAYMENT_SUC_WINDOW = "dws_trade_payment_suc_window";
    String DWS_TRADE_ORDER_WINDOW = "dws_trade_order_window";
    String DWS_TRADE_SKU_ORDER_WINDOW = "dws_trade_sku_order_window";
    String DWS_TRADE_PROVINCE_ORDER_WINDOW = "dws_trade_province_order_window";
    String DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW ="dws_trade_trademark_category_user_refund_window";


    long SEVEN_DAY_MS = 7 * 24 * 60 * 60 * 1000;
    int TWO_DAY_SECONDS = 2 * 24 * 60 * 60;
}