"""
Schéma (noms de colonnes) des tables TPCx-BB.

POURQUOI CE FICHIER ?
    Les fichiers .dat du kit n'ont PAS de ligne d'en-tête : seules les valeurs sont présentes.
    Les noms de colonnes proviennent du DDL de référence TPCx-BB (création des tables Hive de BigBench),
    lui-même repris de TPC-DS pour les tables structurées.

    Statut : DÉDUIT. Le nombre de colonnes de chaque fichier a été VÉRIFIÉ (comptage des séparateurs,
    nombre constant sur toutes les lignes). La correspondance nom <-> position est confirmée par des
    contrôles de cohérence (ex. ss_ext_sales_price = ss_sales_price * ss_quantity) dans le script 02.
"""

SCHEMA = {
    # ---------------- Tables reprises de TPC-DS ----------------
    "customer": ["c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk", "c_current_addr_sk",
                 "c_first_shipto_date_sk", "c_first_sales_date_sk", "c_salutation", "c_first_name", "c_last_name",
                 "c_preferred_cust_flag", "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country",
                 "c_login", "c_email_address", "c_last_review_date"],
    "customer_address": ["ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name", "ca_street_type",
                         "ca_suite_number", "ca_city", "ca_county", "ca_state", "ca_zip", "ca_country",
                         "ca_gmt_offset", "ca_location_type"],
    "customer_demographics": ["cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status",
                              "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count", "cd_dep_employed_count",
                              "cd_dep_college_count"],
    "date_dim": ["d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq", "d_quarter_seq", "d_year", "d_dow",
                 "d_moy", "d_dom", "d_qoy", "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name",
                 "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday", "d_first_dom", "d_last_dom",
                 "d_same_day_ly", "d_same_day_lq", "d_current_day", "d_current_week", "d_current_month",
                 "d_current_quarter", "d_current_year"],
    "household_demographics": ["hd_demo_sk", "hd_income_band_sk", "hd_buy_potential", "hd_dep_count",
                               "hd_vehicle_count"],
    "income_band": ["ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"],
    "inventory": ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand"],
    "item": ["i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date", "i_item_desc", "i_current_price",
             "i_wholesale_cost", "i_brand_id", "i_brand", "i_class_id", "i_class", "i_category_id", "i_category",
             "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color", "i_units", "i_container",
             "i_manager_id", "i_product_name"],
    "promotion": ["p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk", "p_item_sk", "p_cost",
                  "p_response_target", "p_promo_name", "p_channel_dmail", "p_channel_email", "p_channel_catalog",
                  "p_channel_tv", "p_channel_radio", "p_channel_press", "p_channel_event", "p_channel_demo",
                  "p_channel_details", "p_purpose", "p_discount_active"],
    "reason": ["r_reason_sk", "r_reason_id", "r_reason_desc"],
    "ship_mode": ["sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code", "sm_carrier", "sm_contract"],
    "store": ["s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date", "s_closed_date_sk", "s_store_name",
              "s_number_employees", "s_floor_space", "s_hours", "s_manager", "s_market_id", "s_geography_class",
              "s_market_desc", "s_market_manager", "s_division_id", "s_division_name", "s_company_id",
              "s_company_name", "s_street_number", "s_street_name", "s_street_type", "s_suite_number", "s_city",
              "s_county", "s_state", "s_zip", "s_country", "s_gmt_offset", "s_tax_precentage"],
    "store_returns": ["sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk", "sr_customer_sk", "sr_cdemo_sk",
                      "sr_hdemo_sk", "sr_addr_sk", "sr_store_sk", "sr_reason_sk", "sr_ticket_number",
                      "sr_return_quantity", "sr_return_amt", "sr_return_tax", "sr_return_amt_inc_tax", "sr_fee",
                      "sr_return_ship_cost", "sr_refunded_cash", "sr_reversed_charge", "sr_store_credit",
                      "sr_net_loss"],
    "store_sales": ["ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk",
                    "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk", "ss_ticket_number", "ss_quantity",
                    "ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt",
                    "ss_ext_sales_price", "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax",
                    "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"],
    "time_dim": ["t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute", "t_second", "t_am_pm", "t_shift",
                 "t_sub_shift", "t_meal_time"],
    "warehouse": ["w_warehouse_sk", "w_warehouse_id", "w_warehouse_name", "w_warehouse_sq_ft", "w_street_number",
                  "w_street_name", "w_street_type", "w_suite_number", "w_city", "w_county", "w_state", "w_zip",
                  "w_country", "w_gmt_offset"],
    "web_page": ["wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date", "wp_rec_end_date", "wp_creation_date_sk",
                 "wp_access_date_sk", "wp_autogen_flag", "wp_customer_sk", "wp_url", "wp_type", "wp_char_count",
                 "wp_link_count", "wp_image_count", "wp_max_ad_count"],
    "web_returns": ["wr_returned_date_sk", "wr_returned_time_sk", "wr_item_sk", "wr_refunded_customer_sk",
                    "wr_refunded_cdemo_sk", "wr_refunded_hdemo_sk", "wr_refunded_addr_sk", "wr_returning_customer_sk",
                    "wr_returning_cdemo_sk", "wr_returning_hdemo_sk", "wr_returning_addr_sk", "wr_web_page_sk",
                    "wr_reason_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt", "wr_return_tax",
                    "wr_return_amt_inc_tax", "wr_fee", "wr_return_ship_cost", "wr_refunded_cash",
                    "wr_reversed_charge", "wr_account_credit", "wr_net_loss"],
    "web_sales": ["ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk", "ws_bill_customer_sk",
                  "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk", "ws_ship_customer_sk",
                  "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk", "ws_web_page_sk", "ws_web_site_sk",
                  "ws_ship_mode_sk", "ws_warehouse_sk", "ws_promo_sk", "ws_order_number", "ws_quantity",
                  "ws_wholesale_cost", "ws_list_price", "ws_sales_price", "ws_ext_discount_amt",
                  "ws_ext_sales_price", "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax", "ws_coupon_amt",
                  "ws_ext_ship_cost", "ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship",
                  "ws_net_paid_inc_ship_tax", "ws_net_profit"],
    "web_site": ["web_site_sk", "web_site_id", "web_rec_start_date", "web_rec_end_date", "web_name",
                 "web_open_date_sk", "web_close_date_sk", "web_class", "web_manager", "web_mkt_id", "web_mkt_class",
                 "web_mkt_desc", "web_market_manager", "web_company_id", "web_company_name", "web_street_number",
                 "web_street_name", "web_street_type", "web_suite_number", "web_city", "web_county", "web_state",
                 "web_zip", "web_country", "web_gmt_offset", "web_tax_percentage"],
    # ---------------- Tables propres à TPCx-BB (BigBench) ----------------
    "web_clickstreams": ["wcs_click_date_sk", "wcs_click_time_sk", "wcs_sales_sk", "wcs_item_sk",
                         "wcs_web_page_sk", "wcs_user_sk"],
    "product_reviews": ["pr_review_sk", "pr_review_date", "pr_review_time", "pr_review_rating", "pr_item_sk",
                        "pr_user_sk", "pr_order_sk", "pr_review_content"],
    "item_marketprices": ["imp_sk", "imp_item_sk", "imp_competitor", "imp_competitor_price", "imp_start_date",
                          "imp_end_date"],
}

# Tables spécifiques à BigBench (non présentes dans TPC-DS)
BIGBENCH_ONLY = {"web_clickstreams", "product_reviews", "item_marketprices"}

# ---------------------------------------------------------------------------
# Référence : TPCx-BB Standard Specification v1.7.0, Appendix B (p. 62-72)
#   « If there is a conflict between the descriptions provided in this TPCx-BB Specification
#     and the TPCx-BB kit implementation, the TPCx-BB kit prevails. » (p. 62)
# ---------------------------------------------------------------------------
SPEC_VERSION = "TPCx-BB Standard Specification v1.7.0, Appendix B"

# Colonnes déclarées NOT NULL dans la spécification (B.1, colonnes utilisées par les requêtes)
SPEC_NOT_NULL = {
    "date_dim": ["d_date_sk"], "time_dim": ["t_time_sk"],
    "customer": ["c_customer_sk", "c_customer_id"], "customer_address": ["ca_address_sk"],
    "customer_demographics": ["cd_demo_sk"], "household_demographics": ["hd_demo_sk"],
    "item": ["i_item_sk", "i_item_id"], "item_marketprices": ["imp_item_sk"],
    "inventory": ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
    "product_reviews": ["pr_review_sk", "pr_review_rating", "pr_item_sk", "pr_review_content"],
    "store": ["s_store_sk", "s_store_id"], "store_sales": ["ss_item_sk", "ss_ticket_number"],
    "store_returns": ["sr_item_sk", "sr_ticket_number"], "web_sales": ["ws_item_sk", "ws_order_number"],
    "web_returns": ["wr_item_sk"], "warehouse": ["w_warehouse_sk"], "web_page": ["wp_web_page_sk"],
}

# Colonnes décrites par la spécification mais ABSENTES des fichiers du kit (le kit prévaut)
SPEC_COLONNES_ABSENTES = {"web_sales": ["ws_sk"], "web_clickstreams": ["wcs_click_sk"]}

# Types de référence de la spécification pour les mesures (DuckDB les lit en DOUBLE / BIGINT)
SPEC_TYPES = {"montants (*_price, *_amt, *_cost, *_paid, *_profit)": "DECIMAL(7,2)",
              "quantités (*_quantity, *_quantity_on_hand)": "INTEGER", "clés (*_sk)": "BIGINT",
              "identifiants métier (*_id)": "CHAR(16)", "pr_review_content": "TEXT"}


def read_sql(path: str, table: str) -> str:
    """Expression DuckDB lisant un fichier .dat sans en-tête avec les noms de colonnes du DDL."""
    cols = SCHEMA[table]
    names = ", ".join(f"'{c}'" for c in cols)
    return (f"read_csv('{path}', delim='|', header=false, names=[{names}], "
            f"sample_size=-1, quote='', escape='')")
