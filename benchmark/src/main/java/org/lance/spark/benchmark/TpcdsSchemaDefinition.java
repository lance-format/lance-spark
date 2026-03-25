/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.benchmark;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class TpcdsSchemaDefinition {

  private TpcdsSchemaDefinition() {}

  private static final Map<String, StructType> SCHEMAS;

  static {
    Map<String, StructType> m = new LinkedHashMap<>();

    m.put(
        "call_center",
        new StructType(
            new StructField[] {
              field("cc_call_center_sk", DataTypes.LongType),
              field("cc_call_center_id", DataTypes.StringType),
              field("cc_rec_start_date", DataTypes.DateType),
              field("cc_rec_end_date", DataTypes.DateType),
              field("cc_closed_date_sk", DataTypes.LongType),
              field("cc_open_date_sk", DataTypes.LongType),
              field("cc_name", DataTypes.StringType),
              field("cc_class", DataTypes.StringType),
              field("cc_employees", DataTypes.IntegerType),
              field("cc_sq_ft", DataTypes.IntegerType),
              field("cc_hours", DataTypes.StringType),
              field("cc_manager", DataTypes.StringType),
              field("cc_mkt_id", DataTypes.IntegerType),
              field("cc_mkt_class", DataTypes.StringType),
              field("cc_mkt_desc", DataTypes.StringType),
              field("cc_market_manager", DataTypes.StringType),
              field("cc_division", DataTypes.IntegerType),
              field("cc_division_name", DataTypes.StringType),
              field("cc_company", DataTypes.IntegerType),
              field("cc_company_name", DataTypes.StringType),
              field("cc_street_number", DataTypes.StringType),
              field("cc_street_name", DataTypes.StringType),
              field("cc_street_type", DataTypes.StringType),
              field("cc_suite_number", DataTypes.StringType),
              field("cc_city", DataTypes.StringType),
              field("cc_county", DataTypes.StringType),
              field("cc_state", DataTypes.StringType),
              field("cc_zip", DataTypes.StringType),
              field("cc_country", DataTypes.StringType),
              field("cc_gmt_offset", DataTypes.createDecimalType(5, 2)),
              field("cc_tax_percentage", DataTypes.createDecimalType(5, 2)),
            }));

    m.put(
        "catalog_page",
        new StructType(
            new StructField[] {
              field("cp_catalog_page_sk", DataTypes.LongType),
              field("cp_catalog_page_id", DataTypes.StringType),
              field("cp_start_date_sk", DataTypes.LongType),
              field("cp_end_date_sk", DataTypes.LongType),
              field("cp_department", DataTypes.StringType),
              field("cp_catalog_number", DataTypes.IntegerType),
              field("cp_catalog_page_number", DataTypes.IntegerType),
              field("cp_description", DataTypes.StringType),
              field("cp_type", DataTypes.StringType),
            }));

    m.put(
        "catalog_returns",
        new StructType(
            new StructField[] {
              field("cr_returned_date_sk", DataTypes.LongType),
              field("cr_returned_time_sk", DataTypes.LongType),
              field("cr_item_sk", DataTypes.LongType),
              field("cr_refunded_customer_sk", DataTypes.LongType),
              field("cr_refunded_cdemo_sk", DataTypes.LongType),
              field("cr_refunded_hdemo_sk", DataTypes.LongType),
              field("cr_refunded_addr_sk", DataTypes.LongType),
              field("cr_returning_customer_sk", DataTypes.LongType),
              field("cr_returning_cdemo_sk", DataTypes.LongType),
              field("cr_returning_hdemo_sk", DataTypes.LongType),
              field("cr_returning_addr_sk", DataTypes.LongType),
              field("cr_call_center_sk", DataTypes.LongType),
              field("cr_catalog_page_sk", DataTypes.LongType),
              field("cr_ship_mode_sk", DataTypes.LongType),
              field("cr_warehouse_sk", DataTypes.LongType),
              field("cr_reason_sk", DataTypes.LongType),
              field("cr_order_number", DataTypes.LongType),
              field("cr_return_quantity", DataTypes.IntegerType),
              field("cr_return_amount", DataTypes.createDecimalType(7, 2)),
              field("cr_return_tax", DataTypes.createDecimalType(7, 2)),
              field("cr_return_amt_inc_tax", DataTypes.createDecimalType(7, 2)),
              field("cr_fee", DataTypes.createDecimalType(7, 2)),
              field("cr_return_ship_cost", DataTypes.createDecimalType(7, 2)),
              field("cr_refunded_cash", DataTypes.createDecimalType(7, 2)),
              field("cr_reversed_charge", DataTypes.createDecimalType(7, 2)),
              field("cr_store_credit", DataTypes.createDecimalType(7, 2)),
              field("cr_net_loss", DataTypes.createDecimalType(7, 2)),
            }));

    m.put(
        "catalog_sales",
        new StructType(
            new StructField[] {
              field("cs_sold_date_sk", DataTypes.LongType),
              field("cs_sold_time_sk", DataTypes.LongType),
              field("cs_ship_date_sk", DataTypes.LongType),
              field("cs_bill_customer_sk", DataTypes.LongType),
              field("cs_bill_cdemo_sk", DataTypes.LongType),
              field("cs_bill_hdemo_sk", DataTypes.LongType),
              field("cs_bill_addr_sk", DataTypes.LongType),
              field("cs_ship_customer_sk", DataTypes.LongType),
              field("cs_ship_cdemo_sk", DataTypes.LongType),
              field("cs_ship_hdemo_sk", DataTypes.LongType),
              field("cs_ship_addr_sk", DataTypes.LongType),
              field("cs_call_center_sk", DataTypes.LongType),
              field("cs_catalog_page_sk", DataTypes.LongType),
              field("cs_ship_mode_sk", DataTypes.LongType),
              field("cs_warehouse_sk", DataTypes.LongType),
              field("cs_item_sk", DataTypes.LongType),
              field("cs_promo_sk", DataTypes.LongType),
              field("cs_order_number", DataTypes.LongType),
              field("cs_quantity", DataTypes.IntegerType),
              field("cs_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("cs_list_price", DataTypes.createDecimalType(7, 2)),
              field("cs_sales_price", DataTypes.createDecimalType(7, 2)),
              field("cs_ext_discount_amt", DataTypes.createDecimalType(7, 2)),
              field("cs_ext_sales_price", DataTypes.createDecimalType(7, 2)),
              field("cs_ext_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("cs_ext_list_price", DataTypes.createDecimalType(7, 2)),
              field("cs_ext_tax", DataTypes.createDecimalType(7, 2)),
              field("cs_coupon_amt", DataTypes.createDecimalType(7, 2)),
              field("cs_ext_ship_cost", DataTypes.createDecimalType(7, 2)),
              field("cs_net_paid", DataTypes.createDecimalType(7, 2)),
              field("cs_net_paid_inc_tax", DataTypes.createDecimalType(7, 2)),
              field("cs_net_paid_inc_ship", DataTypes.createDecimalType(7, 2)),
              field("cs_net_paid_inc_ship_tax", DataTypes.createDecimalType(7, 2)),
              field("cs_net_profit", DataTypes.createDecimalType(7, 2)),
            }));

    m.put(
        "customer",
        new StructType(
            new StructField[] {
              field("c_customer_sk", DataTypes.LongType),
              field("c_customer_id", DataTypes.StringType),
              field("c_current_cdemo_sk", DataTypes.LongType),
              field("c_current_hdemo_sk", DataTypes.LongType),
              field("c_current_addr_sk", DataTypes.LongType),
              field("c_first_shipto_date_sk", DataTypes.LongType),
              field("c_first_sales_date_sk", DataTypes.LongType),
              field("c_salutation", DataTypes.StringType),
              field("c_first_name", DataTypes.StringType),
              field("c_last_name", DataTypes.StringType),
              field("c_preferred_cust_flag", DataTypes.StringType),
              field("c_birth_day", DataTypes.IntegerType),
              field("c_birth_month", DataTypes.IntegerType),
              field("c_birth_year", DataTypes.IntegerType),
              field("c_birth_country", DataTypes.StringType),
              field("c_login", DataTypes.StringType),
              field("c_email_address", DataTypes.StringType),
              field("c_last_review_date_sk", DataTypes.LongType),
            }));

    m.put(
        "customer_address",
        new StructType(
            new StructField[] {
              field("ca_address_sk", DataTypes.LongType),
              field("ca_address_id", DataTypes.StringType),
              field("ca_street_number", DataTypes.StringType),
              field("ca_street_name", DataTypes.StringType),
              field("ca_street_type", DataTypes.StringType),
              field("ca_suite_number", DataTypes.StringType),
              field("ca_city", DataTypes.StringType),
              field("ca_county", DataTypes.StringType),
              field("ca_state", DataTypes.StringType),
              field("ca_zip", DataTypes.StringType),
              field("ca_country", DataTypes.StringType),
              field("ca_gmt_offset", DataTypes.createDecimalType(5, 2)),
              field("ca_location_type", DataTypes.StringType),
            }));

    m.put(
        "customer_demographics",
        new StructType(
            new StructField[] {
              field("cd_demo_sk", DataTypes.LongType),
              field("cd_gender", DataTypes.StringType),
              field("cd_marital_status", DataTypes.StringType),
              field("cd_education_status", DataTypes.StringType),
              field("cd_purchase_estimate", DataTypes.IntegerType),
              field("cd_credit_rating", DataTypes.StringType),
              field("cd_dep_count", DataTypes.IntegerType),
              field("cd_dep_employed_count", DataTypes.IntegerType),
              field("cd_dep_college_count", DataTypes.IntegerType),
            }));

    m.put(
        "date_dim",
        new StructType(
            new StructField[] {
              field("d_date_sk", DataTypes.LongType),
              field("d_date_id", DataTypes.StringType),
              field("d_date", DataTypes.DateType),
              field("d_month_seq", DataTypes.IntegerType),
              field("d_week_seq", DataTypes.IntegerType),
              field("d_quarter_seq", DataTypes.IntegerType),
              field("d_year", DataTypes.IntegerType),
              field("d_dow", DataTypes.IntegerType),
              field("d_moy", DataTypes.IntegerType),
              field("d_dom", DataTypes.IntegerType),
              field("d_qoy", DataTypes.IntegerType),
              field("d_fy_year", DataTypes.IntegerType),
              field("d_fy_quarter_seq", DataTypes.IntegerType),
              field("d_fy_week_seq", DataTypes.IntegerType),
              field("d_day_name", DataTypes.StringType),
              field("d_quarter_name", DataTypes.StringType),
              field("d_holiday", DataTypes.StringType),
              field("d_weekend", DataTypes.StringType),
              field("d_following_holiday", DataTypes.StringType),
              field("d_first_dom", DataTypes.IntegerType),
              field("d_last_dom", DataTypes.IntegerType),
              field("d_same_day_ly", DataTypes.IntegerType),
              field("d_same_day_lq", DataTypes.IntegerType),
              field("d_current_day", DataTypes.StringType),
              field("d_current_week", DataTypes.StringType),
              field("d_current_month", DataTypes.StringType),
              field("d_current_quarter", DataTypes.StringType),
              field("d_current_year", DataTypes.StringType),
            }));

    m.put(
        "household_demographics",
        new StructType(
            new StructField[] {
              field("hd_demo_sk", DataTypes.LongType),
              field("hd_income_band_sk", DataTypes.LongType),
              field("hd_buy_potential", DataTypes.StringType),
              field("hd_dep_count", DataTypes.IntegerType),
              field("hd_vehicle_count", DataTypes.IntegerType),
            }));

    m.put(
        "income_band",
        new StructType(
            new StructField[] {
              field("ib_income_band_sk", DataTypes.LongType),
              field("ib_lower_bound", DataTypes.IntegerType),
              field("ib_upper_bound", DataTypes.IntegerType),
            }));

    m.put(
        "inventory",
        new StructType(
            new StructField[] {
              field("inv_date_sk", DataTypes.LongType),
              field("inv_item_sk", DataTypes.LongType),
              field("inv_warehouse_sk", DataTypes.LongType),
              field("inv_quantity_on_hand", DataTypes.IntegerType),
            }));

    m.put(
        "item",
        new StructType(
            new StructField[] {
              field("i_item_sk", DataTypes.LongType),
              field("i_item_id", DataTypes.StringType),
              field("i_rec_start_date", DataTypes.DateType),
              field("i_rec_end_date", DataTypes.DateType),
              field("i_item_desc", DataTypes.StringType),
              field("i_current_price", DataTypes.createDecimalType(7, 2)),
              field("i_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("i_brand_id", DataTypes.IntegerType),
              field("i_brand", DataTypes.StringType),
              field("i_class_id", DataTypes.IntegerType),
              field("i_class", DataTypes.StringType),
              field("i_category_id", DataTypes.IntegerType),
              field("i_category", DataTypes.StringType),
              field("i_manufact_id", DataTypes.IntegerType),
              field("i_manufact", DataTypes.StringType),
              field("i_size", DataTypes.StringType),
              field("i_formulation", DataTypes.StringType),
              field("i_color", DataTypes.StringType),
              field("i_units", DataTypes.StringType),
              field("i_container", DataTypes.StringType),
              field("i_manager_id", DataTypes.IntegerType),
              field("i_product_name", DataTypes.StringType),
            }));

    m.put(
        "promotion",
        new StructType(
            new StructField[] {
              field("p_promo_sk", DataTypes.LongType),
              field("p_promo_id", DataTypes.StringType),
              field("p_start_date_sk", DataTypes.LongType),
              field("p_end_date_sk", DataTypes.LongType),
              field("p_item_sk", DataTypes.LongType),
              field("p_cost", DataTypes.createDecimalType(15, 2)),
              field("p_response_target", DataTypes.IntegerType),
              field("p_promo_name", DataTypes.StringType),
              field("p_channel_dmail", DataTypes.StringType),
              field("p_channel_email", DataTypes.StringType),
              field("p_channel_catalog", DataTypes.StringType),
              field("p_channel_tv", DataTypes.StringType),
              field("p_channel_radio", DataTypes.StringType),
              field("p_channel_press", DataTypes.StringType),
              field("p_channel_event", DataTypes.StringType),
              field("p_channel_demo", DataTypes.StringType),
              field("p_channel_details", DataTypes.StringType),
              field("p_purpose", DataTypes.StringType),
              field("p_discount_active", DataTypes.StringType),
            }));

    m.put(
        "reason",
        new StructType(
            new StructField[] {
              field("r_reason_sk", DataTypes.LongType),
              field("r_reason_id", DataTypes.StringType),
              field("r_reason_desc", DataTypes.StringType),
            }));

    m.put(
        "ship_mode",
        new StructType(
            new StructField[] {
              field("sm_ship_mode_sk", DataTypes.LongType),
              field("sm_ship_mode_id", DataTypes.StringType),
              field("sm_type", DataTypes.StringType),
              field("sm_code", DataTypes.StringType),
              field("sm_carrier", DataTypes.StringType),
              field("sm_contract", DataTypes.StringType),
            }));

    m.put(
        "store",
        new StructType(
            new StructField[] {
              field("s_store_sk", DataTypes.LongType),
              field("s_store_id", DataTypes.StringType),
              field("s_rec_start_date", DataTypes.DateType),
              field("s_rec_end_date", DataTypes.DateType),
              field("s_closed_date_sk", DataTypes.LongType),
              field("s_store_name", DataTypes.StringType),
              field("s_number_employees", DataTypes.IntegerType),
              field("s_floor_space", DataTypes.IntegerType),
              field("s_hours", DataTypes.StringType),
              field("s_manager", DataTypes.StringType),
              field("s_market_id", DataTypes.IntegerType),
              field("s_geography_class", DataTypes.StringType),
              field("s_market_desc", DataTypes.StringType),
              field("s_market_manager", DataTypes.StringType),
              field("s_division_id", DataTypes.IntegerType),
              field("s_division_name", DataTypes.StringType),
              field("s_company_id", DataTypes.IntegerType),
              field("s_company_name", DataTypes.StringType),
              field("s_street_number", DataTypes.StringType),
              field("s_street_name", DataTypes.StringType),
              field("s_street_type", DataTypes.StringType),
              field("s_suite_number", DataTypes.StringType),
              field("s_city", DataTypes.StringType),
              field("s_county", DataTypes.StringType),
              field("s_state", DataTypes.StringType),
              field("s_zip", DataTypes.StringType),
              field("s_country", DataTypes.StringType),
              field("s_gmt_offset", DataTypes.createDecimalType(5, 2)),
              field("s_tax_precentage", DataTypes.createDecimalType(5, 2)),
            }));

    m.put(
        "store_returns",
        new StructType(
            new StructField[] {
              field("sr_returned_date_sk", DataTypes.LongType),
              field("sr_return_time_sk", DataTypes.LongType),
              field("sr_item_sk", DataTypes.LongType),
              field("sr_customer_sk", DataTypes.LongType),
              field("sr_cdemo_sk", DataTypes.LongType),
              field("sr_hdemo_sk", DataTypes.LongType),
              field("sr_addr_sk", DataTypes.LongType),
              field("sr_store_sk", DataTypes.LongType),
              field("sr_reason_sk", DataTypes.LongType),
              field("sr_ticket_number", DataTypes.LongType),
              field("sr_return_quantity", DataTypes.IntegerType),
              field("sr_return_amt", DataTypes.createDecimalType(7, 2)),
              field("sr_return_tax", DataTypes.createDecimalType(7, 2)),
              field("sr_return_amt_inc_tax", DataTypes.createDecimalType(7, 2)),
              field("sr_fee", DataTypes.createDecimalType(7, 2)),
              field("sr_return_ship_cost", DataTypes.createDecimalType(7, 2)),
              field("sr_refunded_cash", DataTypes.createDecimalType(7, 2)),
              field("sr_reversed_charge", DataTypes.createDecimalType(7, 2)),
              field("sr_store_credit", DataTypes.createDecimalType(7, 2)),
              field("sr_net_loss", DataTypes.createDecimalType(7, 2)),
            }));

    m.put(
        "store_sales",
        new StructType(
            new StructField[] {
              field("ss_sold_date_sk", DataTypes.LongType),
              field("ss_sold_time_sk", DataTypes.LongType),
              field("ss_item_sk", DataTypes.LongType),
              field("ss_customer_sk", DataTypes.LongType),
              field("ss_cdemo_sk", DataTypes.LongType),
              field("ss_hdemo_sk", DataTypes.LongType),
              field("ss_addr_sk", DataTypes.LongType),
              field("ss_store_sk", DataTypes.LongType),
              field("ss_promo_sk", DataTypes.LongType),
              field("ss_ticket_number", DataTypes.LongType),
              field("ss_quantity", DataTypes.IntegerType),
              field("ss_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("ss_list_price", DataTypes.createDecimalType(7, 2)),
              field("ss_sales_price", DataTypes.createDecimalType(7, 2)),
              field("ss_ext_discount_amt", DataTypes.createDecimalType(7, 2)),
              field("ss_ext_sales_price", DataTypes.createDecimalType(7, 2)),
              field("ss_ext_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("ss_ext_list_price", DataTypes.createDecimalType(7, 2)),
              field("ss_ext_tax", DataTypes.createDecimalType(7, 2)),
              field("ss_coupon_amt", DataTypes.createDecimalType(7, 2)),
              field("ss_net_paid", DataTypes.createDecimalType(7, 2)),
              field("ss_net_paid_inc_tax", DataTypes.createDecimalType(7, 2)),
              field("ss_net_profit", DataTypes.createDecimalType(7, 2)),
            }));

    m.put(
        "time_dim",
        new StructType(
            new StructField[] {
              field("t_time_sk", DataTypes.LongType),
              field("t_time_id", DataTypes.StringType),
              field("t_time", DataTypes.IntegerType),
              field("t_hour", DataTypes.IntegerType),
              field("t_minute", DataTypes.IntegerType),
              field("t_second", DataTypes.IntegerType),
              field("t_am_pm", DataTypes.StringType),
              field("t_shift", DataTypes.StringType),
              field("t_sub_shift", DataTypes.StringType),
              field("t_meal_time", DataTypes.StringType),
            }));

    m.put(
        "warehouse",
        new StructType(
            new StructField[] {
              field("w_warehouse_sk", DataTypes.LongType),
              field("w_warehouse_id", DataTypes.StringType),
              field("w_warehouse_name", DataTypes.StringType),
              field("w_warehouse_sq_ft", DataTypes.IntegerType),
              field("w_street_number", DataTypes.StringType),
              field("w_street_name", DataTypes.StringType),
              field("w_street_type", DataTypes.StringType),
              field("w_suite_number", DataTypes.StringType),
              field("w_city", DataTypes.StringType),
              field("w_county", DataTypes.StringType),
              field("w_state", DataTypes.StringType),
              field("w_zip", DataTypes.StringType),
              field("w_country", DataTypes.StringType),
              field("w_gmt_offset", DataTypes.createDecimalType(5, 2)),
            }));

    m.put(
        "web_page",
        new StructType(
            new StructField[] {
              field("wp_web_page_sk", DataTypes.LongType),
              field("wp_web_page_id", DataTypes.StringType),
              field("wp_rec_start_date", DataTypes.DateType),
              field("wp_rec_end_date", DataTypes.DateType),
              field("wp_creation_date_sk", DataTypes.LongType),
              field("wp_access_date_sk", DataTypes.LongType),
              field("wp_autogen_flag", DataTypes.StringType),
              field("wp_customer_sk", DataTypes.LongType),
              field("wp_url", DataTypes.StringType),
              field("wp_type", DataTypes.StringType),
              field("wp_char_count", DataTypes.IntegerType),
              field("wp_link_count", DataTypes.IntegerType),
              field("wp_image_count", DataTypes.IntegerType),
              field("wp_max_ad_count", DataTypes.IntegerType),
            }));

    m.put(
        "web_returns",
        new StructType(
            new StructField[] {
              field("wr_returned_date_sk", DataTypes.LongType),
              field("wr_returned_time_sk", DataTypes.LongType),
              field("wr_item_sk", DataTypes.LongType),
              field("wr_refunded_customer_sk", DataTypes.LongType),
              field("wr_refunded_cdemo_sk", DataTypes.LongType),
              field("wr_refunded_hdemo_sk", DataTypes.LongType),
              field("wr_refunded_addr_sk", DataTypes.LongType),
              field("wr_returning_customer_sk", DataTypes.LongType),
              field("wr_returning_cdemo_sk", DataTypes.LongType),
              field("wr_returning_hdemo_sk", DataTypes.LongType),
              field("wr_returning_addr_sk", DataTypes.LongType),
              field("wr_web_page_sk", DataTypes.LongType),
              field("wr_reason_sk", DataTypes.LongType),
              field("wr_order_number", DataTypes.LongType),
              field("wr_return_quantity", DataTypes.IntegerType),
              field("wr_return_amt", DataTypes.createDecimalType(7, 2)),
              field("wr_return_tax", DataTypes.createDecimalType(7, 2)),
              field("wr_return_amt_inc_tax", DataTypes.createDecimalType(7, 2)),
              field("wr_fee", DataTypes.createDecimalType(7, 2)),
              field("wr_return_ship_cost", DataTypes.createDecimalType(7, 2)),
              field("wr_refunded_cash", DataTypes.createDecimalType(7, 2)),
              field("wr_reversed_charge", DataTypes.createDecimalType(7, 2)),
              field("wr_account_credit", DataTypes.createDecimalType(7, 2)),
              field("wr_net_loss", DataTypes.createDecimalType(7, 2)),
            }));

    m.put(
        "web_sales",
        new StructType(
            new StructField[] {
              field("ws_sold_date_sk", DataTypes.LongType),
              field("ws_sold_time_sk", DataTypes.LongType),
              field("ws_ship_date_sk", DataTypes.LongType),
              field("ws_item_sk", DataTypes.LongType),
              field("ws_bill_customer_sk", DataTypes.LongType),
              field("ws_bill_cdemo_sk", DataTypes.LongType),
              field("ws_bill_hdemo_sk", DataTypes.LongType),
              field("ws_bill_addr_sk", DataTypes.LongType),
              field("ws_ship_customer_sk", DataTypes.LongType),
              field("ws_ship_cdemo_sk", DataTypes.LongType),
              field("ws_ship_hdemo_sk", DataTypes.LongType),
              field("ws_ship_addr_sk", DataTypes.LongType),
              field("ws_web_page_sk", DataTypes.LongType),
              field("ws_web_site_sk", DataTypes.LongType),
              field("ws_ship_mode_sk", DataTypes.LongType),
              field("ws_warehouse_sk", DataTypes.LongType),
              field("ws_promo_sk", DataTypes.LongType),
              field("ws_order_number", DataTypes.LongType),
              field("ws_quantity", DataTypes.IntegerType),
              field("ws_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("ws_list_price", DataTypes.createDecimalType(7, 2)),
              field("ws_sales_price", DataTypes.createDecimalType(7, 2)),
              field("ws_ext_discount_amt", DataTypes.createDecimalType(7, 2)),
              field("ws_ext_sales_price", DataTypes.createDecimalType(7, 2)),
              field("ws_ext_wholesale_cost", DataTypes.createDecimalType(7, 2)),
              field("ws_ext_list_price", DataTypes.createDecimalType(7, 2)),
              field("ws_ext_tax", DataTypes.createDecimalType(7, 2)),
              field("ws_coupon_amt", DataTypes.createDecimalType(7, 2)),
              field("ws_ext_ship_cost", DataTypes.createDecimalType(7, 2)),
              field("ws_net_paid", DataTypes.createDecimalType(7, 2)),
              field("ws_net_paid_inc_tax", DataTypes.createDecimalType(7, 2)),
              field("ws_net_paid_inc_ship", DataTypes.createDecimalType(7, 2)),
              field("ws_net_paid_inc_ship_tax", DataTypes.createDecimalType(7, 2)),
              field("ws_net_profit", DataTypes.createDecimalType(7, 2)),
            }));

    m.put(
        "web_site",
        new StructType(
            new StructField[] {
              field("web_site_sk", DataTypes.LongType),
              field("web_site_id", DataTypes.StringType),
              field("web_rec_start_date", DataTypes.DateType),
              field("web_rec_end_date", DataTypes.DateType),
              field("web_name", DataTypes.StringType),
              field("web_open_date_sk", DataTypes.LongType),
              field("web_close_date_sk", DataTypes.LongType),
              field("web_class", DataTypes.StringType),
              field("web_manager", DataTypes.StringType),
              field("web_mkt_id", DataTypes.IntegerType),
              field("web_mkt_class", DataTypes.StringType),
              field("web_mkt_desc", DataTypes.StringType),
              field("web_market_manager", DataTypes.StringType),
              field("web_company_id", DataTypes.IntegerType),
              field("web_company_name", DataTypes.StringType),
              field("web_street_number", DataTypes.StringType),
              field("web_street_name", DataTypes.StringType),
              field("web_street_type", DataTypes.StringType),
              field("web_suite_number", DataTypes.StringType),
              field("web_city", DataTypes.StringType),
              field("web_county", DataTypes.StringType),
              field("web_state", DataTypes.StringType),
              field("web_zip", DataTypes.StringType),
              field("web_country", DataTypes.StringType),
              field("web_gmt_offset", DataTypes.createDecimalType(5, 2)),
              field("web_tax_percentage", DataTypes.createDecimalType(5, 2)),
            }));

    m.put(
        "dbgen_version",
        new StructType(
            new StructField[] {
              field("dv_version", DataTypes.StringType),
              field("dv_create_date", DataTypes.DateType),
              field("dv_create_time", DataTypes.StringType),
              field("dv_cmdline_args", DataTypes.StringType),
            }));

    SCHEMAS = Collections.unmodifiableMap(m);
  }

  public static Map<String, StructType> getAllSchemas() {
    return SCHEMAS;
  }

  public static StructType getSchema(String tableName) {
    StructType schema = SCHEMAS.get(tableName);
    if (schema == null) {
      throw new IllegalArgumentException("Unknown TPC-DS table: " + tableName);
    }
    return schema;
  }

  public static String[] getTableNames() {
    return SCHEMAS.keySet().toArray(new String[0]);
  }

  private static StructField field(String name, org.apache.spark.sql.types.DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }
}
