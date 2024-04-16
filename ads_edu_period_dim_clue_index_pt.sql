--create external table if not exists ads_edu.ads_edu_period_dim_clue_index_pt(
--`clue_id` bigint COMMENT '线索id',
--`period_label_id` bigint COMMENT '期标签id',
--`period` string COMMENT '期',
--`first_intention_region_id` bigint COMMENT '首次意向域id',
--`first_intention_id` string COMMENT '首次意向id',
--`first_intention_action` string COMMENT '首次意向动作',
--`first_intention_create_time` string COMMENT '首次意向创建时间',
--`first_intention_order_id` string COMMENT '首次意向订单id',
--`first_intention_sku_id` string COMMENT '首次意向商品id',
--`first_intention_sku_price` string COMMENT '首次意向商品价格',
--`first_intention_sku_first_classify` string COMMENT '首次意向商品一级分类',
--`first_intention_sku_second_classify` string COMMENT '首次意向商品二级分类',
--`first_intention_sku_third_classify` string COMMENT '首次意向商品三级分类',
--`first_intention_channel_type_name` string COMMENT '首次意向渠道类型名',
--`first_intention_source_type` string COMMENT '首次意向来源类型',
--`first_intention_channel1_name` string COMMENT '首次意向一级渠道名',
--`first_intention_channel2_name` string COMMENT '首次意向二级渠道名',
--`first_intention_channel3_name` string COMMENT '首次意向三级渠道名',
--`first_intention_channel4_name` string COMMENT '首次意向四级渠道名',
--`first_intention_channel5_name` string COMMENT '首次意向五级渠道名',
--`clue_channel_type` string COMMENT '线索渠道类型',
--`clue_channel_type_name` string COMMENT '线索渠道类型名',
--`clue_channel1_name` string COMMENT '线索一级渠道名',
--`clue_channel2_name` string COMMENT '线索二级渠道名',
--`clue_channel3_name` string COMMENT '线索三级渠道名',
--`clue_channel4_name` string COMMENT '线索四级渠道名',
--`clue_channel5_name` string COMMENT '线索五级渠道名',
--`phone_hash` string COMMENT '手机号加密串',
--`assign_type` string COMMENT '指派类型',
--`assign_time` string COMMENT '指派时间',
--`assign_end_time` string COMMENT '指派结束时间',
--`sale_id` bigint COMMENT '销售id',
--`sale_name` string COMMENT '销售名',
--`first_org_name` string COMMENT '一级机构名',
--`second_org_name` string COMMENT '二级机构名',
--`third_org_name` string COMMENT '三级机构名',
--`fourth_org_name` string COMMENT '四级机构名',
--`fifth_org_name` string COMMENT '五级机构名',
--`sixth_org_name` string COMMENT '六级机构名',
--`seventh_org_name` string COMMENT '七级机构名',
--`eighth_org_name` string COMMENT '八级机构名',
--`is_new_clue` int COMMENT '是否新线索',
--`is_sale_new_clue` int COMMENT '是否销售新线索',
--`is_first_assign` int COMMENT '是否首次指派',
--`is_period_arrive` int COMMENT '是否期到课',
--`is_period_attendance` int COMMENT '是否期出勤',
--`is_period_finish` int COMMENT '是否期完课',
--`period_drama_convert_amt` decimal(38,2) COMMENT '期直播转化金额',
--`period_convert_amt` decimal(38,2) COMMENT '期转化金额'
--) COMMENT '教育期维度线索指标表(每日全量)'
-- partitioned by (
--`p_date` string COMMENT '日期分区'
--) stored as parquet
-- location '/user/km_km/tables/ads_edu.db/ads_edu_period_dim_clue_index_pd'
set hive.mapred.mode= 'nonstrict';
with base as (
    select
        T1.one_id,
        T1.clue_id,
        T1.period_label_id,
        period,
        first_intention_region_id,
        T1.first_intention_id,
        first_intention_action,
        first_intention_create_time,
        first_intention_order_id,
        T1.first_intention_sku_id,
        first_intention_sku_price,
        first_intention_sku_first_classify,
        first_intention_sku_second_classify,
        first_intention_sku_third_classify,
        first_intention_channel_type_name,
        first_intention_source_type,
        first_intention_channel1_name,
        first_intention_channel2_name,
        first_intention_channel3_name,
        first_intention_channel4_name,
        first_intention_channel5_name,
        clue_channel_type,
        clue_channel_type_name,
        clue_channel1_name,
        clue_channel2_name,
        clue_channel3_name,
        clue_channel4_name,
        clue_channel5_name,
        phone_hash,
        assign_type,
        assign_time,
        assign_end_time,
        sale_id,
        sale_name,
        first_org_name,
        second_org_name,
        third_org_name,
        fourth_org_name,
        fifth_org_name,
        sixth_org_name,
        seventh_org_name,
        eighth_org_name,
        is_new_clue,
        is_sale_new_clue,
        is_first_assign
    from
    (
        select
            clue_id,
            period_label_id,
            period,
            biz_id as first_intention_region_id,
            intention_action as first_intention_action,
            intention_id as first_intention_id,
            create_time as first_intention_create_time,
            order_id as first_intention_order_id,
            sku_id as first_intention_sku_id,

            source_type as first_intention_source_type,
            channel_type_name as first_intention_channel_type_name,
            channel1_name as first_intention_channel1_name,
            channel2_name as first_intention_channel2_name,
            channel3_name as first_intention_channel3_name,
            channel4_name as first_intention_channel4_name,
            channel5_name as first_intention_channel5_name,
            one_id
        from
            dwd_edu.dwd_edu_intention_detail_pt
        where p_date = '${yesterday}'
            and intention_action in (0,11)
            and intention_index=1
    ) T1
    left join
    (
        select
            *
        from
        (
            select
                clue_id,
                first_intention_id,
                first_intention_sku_id,
                actual_start_time
            from
            (
                select
                    clue_id,
                    intention_id as first_intention_id,
                    sku_id as first_intention_sku_id
                from
                    dwd_edu.dwd_edu_intention_detail_pt
                where p_date = '${yesterday}'
                    and intention_action in (0,11)
                    and intention_index=1
            ) inte
            join
            (
                select
                    sku_id,
                    actual_start_time
                from
                (
                    select
                        id as sku_id,
                        course_id
                    from
                        ods.ods_education_sku_pt
                    where p_date = '${yesterday}'
                ) sku
                join
                (
                    select
                        course_id,
                        min(actual_start_time) as actual_start_time
                    from
                        dim.dim_edu_course_section_base_pt
                    where p_date = '${yesterday}'
                        and resource_type='6'
                    group by course_id
                ) course
                on sku.course_id = course.course_id
            ) course_start_time
            on inte.first_intention_sku_id = course_start_time.sku_id
        ) T2
        join
        (
            select
                *
            from
            (
                select
                    clue_id as assign_clue_id,
                    source_type as clue_source_type,
                    channel_type_name as clue_channel_type_name,
                    channel_type_name as clue_channel_type,
                    channel1_name as clue_channel1_name,
                    channel2_name as clue_channel2_name,
                    channel3_name as clue_channel3_name,
                    channel4_name as clue_channel4_name,
                    channel5_name as clue_channel5_name,
                    phone_hash,
                    assign_type,
                    sale_id,
                    sale_name,
                    first_org_name,
                    second_org_name,
                    third_org_name,
                    fourth_org_name,
                    fifth_org_name,
                    sixth_org_name,
                    seventh_org_name,
                    eighth_org_name,
                    is_new as is_new_clue,
                    is_sale_new_clue,
                    is_first_assign,
                    assign_time,
                    assign_end_time,
                    row_number() over(partition by clue_id,assign_time ORDER BY assign_intention_id) rk
                from
                    dws_edu.dws_edu_crm_clue_assign_aggr_pt
                where p_date = '${yesterday}'
            ) T
            where rk = 1
        ) T3
        on T2.clue_id = T3.assign_clue_id
        where T2.actual_start_time between T3.assign_time and T3.assign_end_time
    ) clue
    on T1.first_intention_id = clue.first_intention_id
    left join
    (
        select
            sku_id,
            price as first_intention_sku_price,
            category as first_intention_sku_first_classify,
            sub_category as first_intention_sku_second_classify,
            edu_standard_type_new as first_intention_sku_third_classify

        from
            dim_edu.dim_edu_zhihu_sku_info_pt
        where p_date = '${yesterday}'
    ) sku
    on T1.first_intention_sku_id = sku.sku_id
),

course as (
    select
         inte.one_id,
         inte.category,
         inte.period_label_id,
         max(is_drama_arrive) as is_period_arrive,
         max(is_drama_attendance) as is_period_attendance,
         max(is_drama_finish) as is_period_finish
    from
    (
        select
            one_id,
            category,
            period_label_id
        from
        (
            select
                one_id,
                period_label_id,
                sku_id
            from
                dwd_edu.dwd_edu_intention_detail_pt
            where p_date = '${yesterday}'
        ) T1
        join
        (
            select
                sku_id,
                category
            from
                dim_edu.dim_edu_zhihu_sku_info_pt
            where p_date = '${yesterday}'
        ) T2
        on T1.sku_id = T2.sku_id
    ) inte
    join
    (
        select
            one_id,
            sku_first_classify,
            period_label_id,
            is_drama_arrive,
            is_drama_attendance,
            is_drama_finish
        from
            dws_edu.dws_edu_pref_section_member_learn_progress_wide_pt
        where p_date = '${yesterday}'
            and (is_drama_arrive=1 or is_drama_attendance=1 or is_drama_finish=1)
    ) learn
    on inte.one_id = learn.one_id and inte.category = learn.sku_first_classify and inte.period_label_id = learn.period_label_id
    group by inte.one_id, inte.category, inte.period_label_id
),

clue_order as (
    select
        inte.one_id,
        inte.clue_id,
        clue.paid_time,
        inte.category,
        inte.period_label_id,
        clue.buyer_amount,
        clue.business_number
    from
    (
            select
                one_id,
                category,
                period_label_id,
                clue_id
            from
            (
                select
                    one_id,
                    period_label_id,
                    sku_id,
                    clue_id
                from
                    dwd_edu.dwd_edu_intention_detail_pt
                where p_date = '${yesterday}'
            ) T1
            join
            (
                select
                    sku_id,
                    category
                from
                    dim_edu.dim_edu_zhihu_sku_info_pt
                where p_date = '${yesterday}'
            ) T2
            on T1.sku_id = T2.sku_id
    ) inte
    join
    (
        -- select
        --     clue_id,
        --     from_unixtime(paid_time) as paid_time,
        --     category,
        --     buyer_amount,
        --     business_number
        -- from
        --     dwd_edu.dwd_edu_crm_clue_order_detail_pt
        -- where p_date = '${yesterday}'
        --     and is_gt100_convert=1

        select 
            sc.clue_id
            ,sc.paid_time
            ,category
            ,sc.buyer_amount
            ,ord.payment_no as business_number
           
        from 
        (
            select 
                clue_id
                ,member_id
                ,team_id
                ,team_name
                ,sale_id
                ,sale_name
                ,pay_timestamp as paid_time
                ,buyer_amount
                ,order_id
            from dwd_edu.dwd_edu_sale_clue_order_detail_pt
            where p_date = '${yesterday}'
            and is_gt100_convert=1
        ) sc 
        left join 
        (
            select 
                order_id 
                ,deal_id
                ,payment_no
            from dwd_edu.dwd_edu_trade_order_info_detail_pt
            where p_date = '${yesterday}'
        ) ord on sc.order_id = ord.order_id
        left join 
        (
            select 
                deal_id
                ,trade_no
                ,trade_status as buyable_status
                ,course_id
            from dwd_edu.dwd_edu_trade_wallet_pay_detail_pt
            where p_date = '${yesterday}'
        ) tra on tra.deal_id = ord.deal_id
        left join 
        (
            select 
                commodity_id
                ,category
            from dim_edu.dim_edu_zhihu_sku_info_pt
            where p_date = '${yesterday}'
        ) sku on tra.course_id = sku.commodity_id
        
    ) clue
    on inte.clue_id = clue.clue_id

)

insert overwrite table ads_edu.ads_edu_period_dim_clue_index_pt
partition (p_date = '${yesterday}')
select
    base.clue_id,
    base.period_label_id,
    period,
    first_intention_region_id,
    first_intention_id,
    first_intention_action,
    first_intention_create_time,
    first_intention_order_id,
    first_intention_sku_id,
    first_intention_sku_price,
    first_intention_sku_first_classify,
    first_intention_sku_second_classify,
    first_intention_sku_third_classify,
    first_intention_channel_type_name,
    first_intention_source_type,
    first_intention_channel1_name,
    first_intention_channel2_name,
    first_intention_channel3_name,
    first_intention_channel4_name,
    first_intention_channel5_name,
    clue_channel_type,
    clue_channel_type_name,
    clue_channel1_name,
    clue_channel2_name,
    clue_channel3_name,
    clue_channel4_name,
    clue_channel5_name,
    phone_hash,
    assign_type,
    assign_time,
    assign_end_time,
    sale_id,
    sale_name,
    first_org_name,
    second_org_name,
    third_org_name,
    fourth_org_name,
    fifth_org_name,
    sixth_org_name,
    seventh_org_name,
    eighth_org_name,
    is_new_clue,
    is_sale_new_clue,
    is_first_assign,
    nvl(is_period_arrive,0) as is_period_arrive,
    nvl(is_period_attendance,0) as is_period_attendance,
    nvl(is_period_finish,0) as is_period_finish,
    nvl(period_drama_convert_amt,0) as period_drama_convert_amt,
    nvl(period_convert_amt,0) as period_convert_amt
from
    base
left join
    course
on base.one_id = course.one_id and base.first_intention_sku_first_classify = course.category and base.period_label_id = course.period_label_id
left join
(
    select
         clue_id,
         category,
         period_label_id,
         sum(buyer_amount)/100 as period_drama_convert_amt
    from
    (
        select
            clue_order.clue_id,
            clue_order.category,
            clue_order.period_label_id,

            paid_time,
            buyer_amount,
            business_number
        from
            clue_order
        left join
        (
            select
                one_id,
                sku_first_classify,
                period_label_id,
                actual_start_time,
                actual_end_time
            from
                dws_edu.dws_edu_pref_section_member_learn_progress_wide_pt
            where p_date = '${yesterday}'
                and is_drama_arrive=1
        ) learn
        on clue_order.one_id=learn.one_id and clue_order.category = learn.sku_first_classify and clue_order.period_label_id = learn.period_label_id
        where clue_order.paid_time between learn.actual_start_time and learn.actual_end_time
        group by clue_order.clue_id,clue_order.category,clue_order.period_label_id,
             paid_time,buyer_amount,business_number
    ) T
    group by clue_id, category, period_label_id
) order1
on base.clue_id = order1.clue_id and base.first_intention_sku_first_classify = order1.category and base.period_label_id = order1.period_label_id
left join
(
    select
        T.clue_id,
        T.category,
        T.period_label_id,
        sum(T.buyer_amount)/100 as period_convert_amt
    from
        base
    join
    (
        select
            clue_id,
            paid_time,
            category,
            period_label_id,
            buyer_amount
        from
            clue_order
        group by clue_id, paid_time, category, period_label_id, buyer_amount
    ) T
    on base.clue_id = T.clue_id and base.first_intention_sku_first_classify = T.category and base.period_label_id = T.period_label_id
    where T.paid_time between base.assign_time and base.assign_end_time
    group by T.clue_id, T.category, T.period_label_id
) order2
on base.clue_id = order2.clue_id and base.first_intention_sku_first_classify = order2.category and base.period_label_id = order2.period_label_id

