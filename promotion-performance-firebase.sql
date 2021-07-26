with v as (
SELECT 
--app_info.id  
extract(date FROM timestamp_micros(event_timestamp) AT TIME ZONE "Japan" )  as date 
, platform 
, event_name    
,if (ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="promotion_name") [SAFE_OFFSET(0)] = "news",
    (SELECT item_name FROM UNNEST(items)),
    ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="promotion_name") [SAFE_OFFSET(0)]) as promotion_name    
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="creative_name") [SAFE_OFFSET(0)] AS creative_name
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] AS location_id
, count (event_name) as view_count
FROM `nth-glass-770.analytics_193747159.events_*` 
where event_name = "view_promotion"
-- and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,CURRENT_DATE()-1 )
and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,date_sub(CURRENT_DATE-1 ,INTERVAL 10 DAY ) )
and platform = "IOS"
and ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] = "home"
group by 1,2,3,4,5,6
--order by view_count desc   

UNION ALL 

SELECT 
--app_info.id  
extract(date FROM timestamp_micros(event_timestamp) AT TIME ZONE "Japan" )  as date 
, platform 
, event_name    
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="promotion_name") [SAFE_OFFSET(0)] as promotion_name    
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="creative_name") [SAFE_OFFSET(0)] AS creative_name
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] AS location_id
, count (event_name) as view_count
FROM `nth-glass-770.analytics_193747159.events_*` 
where event_name = "view_promotion"
-- and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,CURRENT_DATE()-1 )
and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,date_sub(CURRENT_DATE-1 ,INTERVAL 10 DAY ) )
and platform = "ANDROID"
and ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] = "home"
group by 1,2,3,4,5,6
--order by view_count desc
)

, s as(
SELECT 
--app_info.id  
extract(date FROM timestamp_micros(event_timestamp) AT TIME ZONE "Japan" )  as date 
, platform 
, event_name    
,if (ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="promotion_name") [SAFE_OFFSET(0)] = "news",
    (SELECT item_name FROM UNNEST(items)),
    ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="promotion_name") [SAFE_OFFSET(0)]) as promotion_name    
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="creative_name") [SAFE_OFFSET(0)] AS creative_name
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] AS location_id
, count (event_name) as select_count
FROM `nth-glass-770.analytics_193747159.events_*` 
where event_name = "select_promotion"
-- and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,CURRENT_DATE()-1 )
and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,date_sub(CURRENT_DATE-1 ,INTERVAL 10 DAY ) )
and platform = "IOS"
and ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] = "home"
group by 1,2,3,4,5,6
--order by select_count desc   

UNION ALL 

SELECT 
--app_info.id  
extract(date FROM timestamp_micros(event_timestamp) AT TIME ZONE "Japan" )  as date 
, platform 
, event_name    
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="promotion_name") [SAFE_OFFSET(0)] as promotion_name    
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="creative_name") [SAFE_OFFSET(0)] AS creative_name
, ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] AS location_id
, count (event_name) as select_count
FROM `nth-glass-770.analytics_193747159.events_*` 
where event_name = "select_promotion"
-- and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,CURRENT_DATE()-1 )
and _TABLE_SUFFIX >= FORMAT_DATE("%Y%m%d" ,date_sub(CURRENT_DATE-1 ,INTERVAL 10 DAY ) )
and platform = "ANDROID"
and ARRAY(SELECT value.string_value FROM UNNEST(event_params) WHERE key="location_id") [SAFE_OFFSET(0)] = "home"
group by 1,2,3,4,5,6
--order by select_count desc
)

SELECT 
v.date 
, v.platform 
, v.promotion_name
, v.creative_name
, v.location_id
, sum(view_count) as view_count
, sum(select_count) as select_count
FROM v 
FULL OUTER JOIN s ON CONCAT(v.date, v.platform, v.promotion_name, v.creative_name) = 
                  CONCAT(s.date, s.platform, s.promotion_name, s.creative_name)
group by 1,2,3,4,5
order by view_count desc

