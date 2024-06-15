WITH 
    diff_returns_f AS (
        SELECT
            1 AS seq_nbr,
            'wt_returns_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                return_reason_sk,
                product_sk,
                return_qty,
                product_amt,
                tax_amt,
                snh_amt,
                discount_amt,
                coupons_amt,
                misc_amt,
                invoice_nbr,
                return_stock_ind,
                return_applied_sk,
                gift_kit_sk,
                seq_nbr,
                reason_return_accepted_sk
            FROM
                hmdb_v2.wt_returns_f
            EXCEPT
            SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                return_reason_sk,
                product_sk,
                return_qty,
                product_amt,
                tax_amt,
                snh_amt,
                discount_amt,
                coupons_amt,
                misc_amt,
                invoice_nbr,
                return_stock_ind,
                return_applied_sk,
                gift_kit_sk,
                seq_nbr,
                reason_return_accepted_sk
            FROM
                hmdb_v3.wt_returns_f)
    ),
    diff_refunds_f AS (
        SELECT
            2 AS seq_nbr,
            'wt_refunds_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
                client_abbr,
                individual_sk,
                accnt_nbr,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                refund_amt,
                refund_method_sk,
                refund_reason_sk,
                refund_src_sk,
                refund_dt,
                reported_undeliverable_sk,
                reaplied_ind,
                mgrt_product_line_sk,
                refund_status_sk
            FROM
                hmdb_v2.wt_refunds_f
            EXCEPT
            SELECT
                client_abbr,
                individual_sk,
                accnt_nbr,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                refund_amt,
                refund_method_sk,
                refund_reason_sk,
                refund_src_sk,
                refund_dt,
                reported_undeliverable_sk,
                reaplied_ind,
                mgrt_product_line_sk,
                refund_status_sk
            FROM
                hmdb_v3.wt_refunds_f)
    ),
    diff_product_cancels_f AS (
        SELECT
            3 AS seq_nbr,
            'wt_product_cancels_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                cancel_reason_sk,
                product_sk,
                cancel_qty,
                cancel_amt,
                tax_amt,
                snh_amt,
                discount_amt,
                coupons_amt,
                misc_amt,
                invoice_nbr,
                gift_kit_sk,
                seq_nbr,
                preview_fee_amt
            FROM
                hmdb_v2.wt_product_cancels_f
            EXCEPT
            SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                cancel_reason_sk,
                product_sk,
                cancel_qty,
                cancel_amt,
                tax_amt,
                snh_amt,
                discount_amt,
                coupons_amt,
                misc_amt,
                invoice_nbr,
                gift_kit_sk,
                seq_nbr,
                preview_fee_amt
            FROM
                hmdb_v3.wt_product_cancels_f)
    ),
    diff_credit_card_charge_f AS (
        SELECT
            4 AS seq_nbr,
            'wt_credit_card_charge_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                charge_sk,
                charge_amt,
                charge_dt,
                invoice_nbr,
                seq_nbr,
                authorization_dt,
                authorization_reference_nbr,
                original_amt
            FROM
                hmdb_v2.wt_credit_card_charge_f
            EXCEPT
            SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                charge_sk,
                charge_amt,
                charge_dt,
                invoice_nbr,
                seq_nbr,
                authorization_dt,
                authorization_reference_nbr,
                original_amt
            FROM
                hmdb_v3.wt_credit_card_charge_f)
    ),
    diff_product_payments_f AS (
        SELECT
            5 AS seq_nbr,
            'wt_product_payments_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                paid_amt,
                payment_method_sk,
                bill_key,
                invoice_nbr,
                application_sk,
                deposit_dt,
                batch_nbr,
                payment_src_sk,
                batch_2_nbr,
                check_nbr
            FROM
                hmdb_v2.wt_product_payments_f
            EXCEPT
            SELECT
                client_abbr,
                accnt_nbr,
                individual_sk,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                order_nbr,
                paid_amt,
                payment_method_sk,
                bill_key,
                invoice_nbr,
                application_sk,
                deposit_dt,
                batch_nbr,
                payment_src_sk,
                batch_2_nbr,
                check_nbr
            FROM
                hmdb_v3.wt_product_payments_f)
    ),
    diff_change_continuity_status_f AS (
        SELECT
            6 AS seq_nbr,
            'wt_change_continuity_status_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
                client_abbr,
                individual_sk,
                accnt_nbr,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                donor_accnt_nbr,
                old_plan_status_sk,
                new_plan_status_sk,
                continuity_plan_sk,
                order_nbr,
                change_reason_sk,
                seq_nbr
            FROM
                hmdb_v2.wt_change_continuity_status_f
            EXCEPT
            SELECT
                client_abbr,
                individual_sk,
                accnt_nbr,
                transaction_nbr,
                operator_id,
                transaction_type_cd,
                product_line_abbr,
                product_line_sk,
                donor_accnt_nbr,
                old_plan_status_sk,
                new_plan_status_sk,
                continuity_plan_sk,
                order_nbr,
                change_reason_sk,
                seq_nbr
            FROM
                hmdb_v3.wt_change_continuity_status_f)
    ),
    diff_wt_order_write_off_f AS (
        SELECT
            7 AS seq_nbr,
            'wt_order_write_off_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    write_off_amt,
    write_off_sk,
    order_nbr,
    order_entry_type_sk,
    service_fee_counter
FROM
    hmdb_v2.wt_order_write_off_f
EXCEPT
SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    write_off_amt,
    write_off_sk,
    order_nbr,
    order_entry_type_sk,
    service_fee_counter
FROM
    hmdb_v3.wt_order_write_off_f)
    ),
    diff_wt_negative_option_generation_f AS (
        SELECT
            8 AS seq_nbr,
            'wt_negative_option_generation_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    product_sk,
    service_dt,
    donee_accnt_nbr,
    continuity_plan_sk,
    announcement_status_sk,
    promotion_key,
    promotion_key_sk,
    period_nbr
FROM
    hmdb_v2.wt_negative_option_generation_f
EXCEPT
SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    product_sk,
    service_dt,
    donee_accnt_nbr,
    continuity_plan_sk,
    announcement_status_sk,
    promotion_key,
    promotion_key_sk,
    period_nbr
FROM
    hmdb_v3.wt_negative_option_generation_f)
    ),
    diff_wt_bill_key_f AS (
        SELECT
            9 AS seq_nbr,
            'wt_bill_key_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    product_line_abbr,
    product_line_sk,
    bill_key,
    mail_dt,
    mailed_qty,
    payment_nbr,
    return_nbr,
    cancels_nbr,
    partial_pay_nbr,
    paid_amt,
    billed_amt,
    total_amt
FROM
    hmdb_v2.wt_bill_key_f
EXCEPT
SELECT
    client_abbr,
    product_line_abbr,
    product_line_sk,
    bill_key,
    mail_dt,
    mailed_qty,
    payment_nbr,
    return_nbr,
    cancels_nbr,
    partial_pay_nbr,
    paid_amt,
    billed_amt,
    total_amt
FROM
    hmdb_v3.wt_bill_key_f)
    ),
    diff_wt_credit_card_refund_charge_f AS (
        SELECT
            10 AS seq_nbr,
            'wt_credit_card_refund_charge_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    order_nbr,
    refund_amt,
    refund_transaction_nbr,
    refund_reason_sk,
    refund_src_sk,
    invoice_nbr,
    seq_nbr,
    refund_charge_sk
FROM
    hmdb_v2.wt_credit_card_refund_charge_f
EXCEPT
SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    order_nbr,
    refund_amt,
    refund_transaction_nbr,
    refund_reason_sk,
    refund_src_sk,
    invoice_nbr,
    seq_nbr,
    refund_charge_sk
FROM
    hmdb_v3.wt_credit_card_refund_charge_f)
    ),
    diff_wt_product_combines_f AS (
        SELECT
            11 AS seq_nbr,
            'wt_product_combines_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    old_accnt_nbr,
    transferred_amt,
    balance_surplus_sk
FROM
    hmdb_v2.wt_product_combines_f
EXCEPT
SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    old_accnt_nbr,
    transferred_amt,
    balance_surplus_sk
FROM
    hmdb_v3.wt_product_combines_f)
    ),
    diff_wt_write_off_reversal_f AS (
        SELECT
            12 AS seq_nbr,
            'wt_write_off_reversal_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    writeoff_transaction_nbr,
    order_nbr,
    write_off_src_sk,
    reversal_amt,
    payment_method_sk,
    invoice_nbr,
    bill_key
FROM
    hmdb_v2.wt_write_off_reversal_f
EXCEPT
SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    writeoff_transaction_nbr,
    order_nbr,
    write_off_src_sk,
    reversal_amt,
    payment_method_sk,
    invoice_nbr,
    bill_key
FROM
    hmdb_v3.wt_write_off_reversal_f)
    ),
    diff_wt_returned_payment_f AS (
        SELECT
            13 AS seq_nbr,
            'wt_returned_payment_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    order_nbr,
    order_payment_sk,
    return_amt,
    return_type_sk,
    return_reason_sk,
    bill_key,
    invoice_nbr,
    bad_payment_order_nbr,
    outstanding_late_fee_amt,
    amt
FROM
    hmdb_v2.wt_returned_payment_f
EXCEPT
SELECT
    client_abbr,
    accnt_nbr,
    individual_sk,
    transaction_nbr,
    operator_id,
    transaction_type_cd,
    product_line_abbr,
    product_line_sk,
    order_nbr,
    order_payment_sk,
    return_amt,
    return_type_sk,
    return_reason_sk,
    bill_key,
    invoice_nbr,
    bad_payment_order_nbr,
    outstanding_late_fee_amt,
    amt
FROM
    hmdb_v3.wt_returned_payment_f)
    ),
    diff_wt_product_order_detail_f AS (
        SELECT
            14 AS seq_nbr,
            'wt_product_order_detail_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    src_update_dt,
    publisher_cd,
    publisher_accnt_nbr,
    individual_sk,
    accnt_nbr,
    product_line_sk,
    order_nbr,
    product_sk,
    product_size,
    product_style,
    product_color,
    order_status_sk,
    service_code_sk,
    order_type_sk,
    payment_type_sk,
    ordered_qty,
    product_amt,
    miscellaneous_charge_sk,
    miscellaneous_charge_amt,
    service_dt,
    donee_ship_to_accnt_nbr,
    reship_dt,
    ftc_first_notice_dt,
    ftc_second_notice_dt,
    ftc_accept_delay_sk,
    not_pulled_reason_sk,
    shipped_status_sk,
    detail_tax_exempt_status_sk,
    actual_ship_dt,
    actual_ship_method_sk,
    distributor_cd,
    product_personalization_sk,
    item_ship_and_handle_amt,
    sequence_within_set,
    returned_items_nbr,
    cancelled_items_nbr,
    reship_qty,
    confirm_application_dt,
    continuity_plan_cd,
    continuity_plan_sk,
    generation_sequence_nbr,
    promotion_key,
    promotion_key_sk,
    label_pull_grouping_cd,
    package_id,
    premium_package,
    original_product_no,
    multimarket_promo_status_sk,
    product_level_tax_amt,
    preview_value_amt,
    preview_days,
    deferred_income_dt,
    due_amt,
    installment_ind,
    installment_amt,
    cc_installment_due_amt,
    order_dt
FROM
    hmdb_v2.wt_product_order_detail_f
EXCEPT
SELECT
    src_update_dt,
    publisher_cd,
    publisher_accnt_nbr,
    individual_sk,
    accnt_nbr,
    product_line_sk,
    order_nbr,
    product_sk,
    product_size,
    product_style,
    product_color,
    order_status_sk,
    service_code_sk,
    order_type_sk,
    payment_type_sk,
    ordered_qty,
    product_amt,
    miscellaneous_charge_sk,
    miscellaneous_charge_amt,
    service_dt,
    donee_ship_to_accnt_nbr,
    reship_dt,
    ftc_first_notice_dt,
    ftc_second_notice_dt,
    ftc_accept_delay_sk,
    not_pulled_reason_sk,
    shipped_status_sk,
    detail_tax_exempt_status_sk,
    actual_ship_dt,
    actual_ship_method_sk,
    distributor_cd,
    product_personalization_sk,
    item_ship_and_handle_amt,
    sequence_within_set,
    returned_items_nbr,
    cancelled_items_nbr,
    reship_qty,
    confirm_application_dt,
    continuity_plan_cd,
    continuity_plan_sk,
    generation_sequence_nbr,
    promotion_key,
    promotion_key_sk,
    label_pull_grouping_cd,
    package_id,
    premium_package,
    original_product_no,
    multimarket_promo_status_sk,
    product_level_tax_amt,
    preview_value_amt,
    preview_days,
    deferred_income_dt,
    due_amt,
    installment_ind,
    installment_amt,
    cc_installment_due_amt,
    order_dt
FROM
    hmdb_v3.wt_product_order_detail_f)
    ),
    diff_wt_product_order_summary_f AS (
        SELECT
            15 AS seq_nbr,
            'wt_product_order_summary_f' AS table_name,
            COUNT(*) AS diff_count
        FROM
            (SELECT
    src_update_dt,
    publisher_cd,
    publisher_accnt_nbr,
    accnt_nbr,
    individual_sk,
    product_line_sk,
    order_nbr,
    operator_id_nbr,
    promotion_key,
    promotion_key_sk,
    order_entry_type_sk,
    credit_status_sk,
    order_status_sk,
    gift_set_type_sk,
    order_source_sk,
    products_ordered_nbr,
    total_order_value_amt,
    total_product_value_amt,
    total_taxes_amt,
    total_postage_and_handling_amt,
    total_miscellaneous_amt,
    total_discount_amt,
    due_amt,
    total_paid_amt,
    total_coupon_amt,
    purchase_order_nbr,
    delivery_method_sk,
    mail_received_dt,
    money_deposit_dt,
    billing_key,
    invoice_nbr,
    last_bill_dt,
    continuity_ind,
    installment_ind,
    installments_allowed_nbr,
    installments_sent_nbr,
    installment_amt,
    cc_installment_due_amt,
    sales_representative_nbr,
    currency_type_sk,
    continuity_plan_cd,
    continuity_plan_sk,
    internet_order_confirmation_nbr,
    system_priced_product_age,
    dispatch_period_nbr,
    price_increased_ind,
    fully_paid_dt,
    invoice_status_sk,
    billing_group,
    late_fees_outstanding_amt,
    state_tax_rate,
    cnty_tax_rate,
    city_tax_rate,
    district_tax_rate,
    preview_amt,
    preset_type,
    preset_amt,
    order_dt
FROM
    hmdb_v2.wt_product_order_summary_f
EXCEPT
SELECT
    src_update_dt,
    publisher_cd,
    publisher_accnt_nbr,
    accnt_nbr,
    individual_sk,
    product_line_sk,
    order_nbr,
    operator_id_nbr,
    promotion_key,
    promotion_key_sk,
    order_entry_type_sk,
    credit_status_sk,
    order_status_sk,
    gift_set_type_sk,
    order_source_sk,
    products_ordered_nbr,
    total_order_value_amt,
    total_product_value_amt,
    total_taxes_amt,
    total_postage_and_handling_amt,
    total_miscellaneous_amt,
    total_discount_amt,
    due_amt,
    total_paid_amt,
    total_coupon_amt,
    purchase_order_nbr,
    delivery_method_sk,
    mail_received_dt,
    money_deposit_dt,
    billing_key,
    invoice_nbr,
    last_bill_dt,
    continuity_ind,
    installment_ind,
    installments_allowed_nbr,
    installments_sent_nbr,
    installment_amt,
    cc_installment_due_amt,
    sales_representative_nbr,
    currency_type_sk,
    continuity_plan_cd,
    continuity_plan_sk,
    internet_order_confirmation_nbr,
    system_priced_product_age,
    dispatch_period_nbr,
    price_increased_ind,
    fully_paid_dt,
    invoice_status_sk,
    billing_group,
    late_fees_outstanding_amt,
    state_tax_rate,
    cnty_tax_rate,
    city_tax_rate,
    district_tax_rate,
    preview_amt,
    preset_type,
    preset_amt,
    order_dt
FROM
    hmdb_v3.wt_product_order_summary_f)
    )

-- Combine all differences into one result set
SELECT * FROM diff_returns_f
UNION ALL
SELECT * FROM diff_refunds_f
UNION ALL
SELECT * FROM diff_product_cancels_f
UNION ALL
SELECT * FROM diff_credit_card_charge_f
UNION ALL
SELECT * FROM diff_product_payments_f
UNION ALL
SELECT * FROM diff_change_continuity_status_f
UNION ALL
select * from diff_wt_order_write_off_f
UNION ALL
select * from diff_wt_negative_option_generation_f
UNION ALL
select * from diff_wt_bill_key_f
UNION ALL
select * from diff_wt_credit_card_refund_charge_f
UNION ALL
select * from diff_wt_product_combines_f
UNION ALL
select * from diff_wt_write_off_reversal_f
UNION ALL
select * from diff_wt_returned_payment_f
UNION ALL
select * from diff_wt_product_order_detail_f
UNION ALL
select * from diff_wt_product_order_summary_f

