{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)", 
    ]
)}}

/*
    AFS Individual Media Aggregation Table
    
    Purpose: Individual-level aggregation of media consumption attributes for analytics.
    Source: v_akkio_attributes_latest
    Grain: One row per AKKIO_ID per PARTITION_DATE
    
    This table aggregates media-related attributes into OBJECT columns:
    - TITLES_WATCHED: Object with title/genre names as keys and counts as values
    - GENRES_WATCHED: Object with genre names as keys and counts as values  
    - NETWORKS_WATCHED: Object with network names as keys and counts as values
    - INPUT_DEVICES_USED: Object with device/service names as keys and counts as values
    - APP_SERVICES_USED: Object with app/service names as keys and counts as values
*/

WITH media_values AS (
    SELECT
        attr.AKKIO_ID,
        attr.PARTITION_DATE,
        
        -- Video Streaming Services (APP_SERVICES_USED)
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN attr.MEDIA_VIDEO_BRAND_NETFLIX IS NOT NULL AND attr.MEDIA_VIDEO_BRAND_NETFLIX != '' THEN 'netflix' END,
            CASE WHEN attr.MEDIA_VIDEO_BRAND_HULU IS NOT NULL AND attr.MEDIA_VIDEO_BRAND_HULU != '' THEN 'hulu' END,
            CASE WHEN attr.MEDIA_VIDEO_BRAND_HBO IS NOT NULL AND attr.MEDIA_VIDEO_BRAND_HBO != '' THEN 'hbo-max' END,
            CASE WHEN attr.MEDIA_VIDEO_BRAND_SLING_TV IS NOT NULL AND attr.MEDIA_VIDEO_BRAND_SLING_TV != '' THEN 'sling-tv' END,
            CASE WHEN attr.MEDIA_VIDEO_BRAND_VUDU IS NOT NULL AND attr.MEDIA_VIDEO_BRAND_VUDU != '' THEN 'vudu' END
        ) AS app_services_array,
        
        -- TV/Cable Providers (NETWORKS_WATCHED)
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN attr.MEDIA_TV_BRAND_COMCAST IS NOT NULL AND attr.MEDIA_TV_BRAND_COMCAST != '' THEN 'comcast' END,
            CASE WHEN attr.MEDIA_TV_BRAND_DIRECTV IS NOT NULL AND attr.MEDIA_TV_BRAND_DIRECTV != '' THEN 'directv' END,
            CASE WHEN attr.MEDIA_TV_BRAND_DISH IS NOT NULL AND attr.MEDIA_TV_BRAND_DISH != '' THEN 'dish-network' END,
            CASE WHEN attr.MEDIA_TV_BRAND_SPECTRUM IS NOT NULL AND attr.MEDIA_TV_BRAND_SPECTRUM != '' THEN 'spectrum' END,
            CASE WHEN attr.MEDIA_TV_BRAND_XFINITY IS NOT NULL AND attr.MEDIA_TV_BRAND_XFINITY != '' THEN 'xfinity' END
        ) AS networks_array,
        
        -- Audio Streaming Services (INPUT_DEVICES_USED)
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN attr.MEDIA_AUDIO_BRAND_PANDORA IS NOT NULL AND attr.MEDIA_AUDIO_BRAND_PANDORA != '' THEN 'pandora' END,
            CASE WHEN attr.MEDIA_AUDIO_BRAND_SIRIUS_XM IS NOT NULL AND attr.MEDIA_AUDIO_BRAND_SIRIUS_XM != '' THEN 'sirius-xm' END,
            CASE WHEN attr.MEDIA_AUDIO_BRAND_SPOTIFY IS NOT NULL AND attr.MEDIA_AUDIO_BRAND_SPOTIFY != '' THEN 'spotify' END
        ) AS audio_array,
        
        -- TV/Movie Genres (GENRES_WATCHED)
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN attr.MEDIA_TV_MOV_HORROR IS NOT NULL AND attr.MEDIA_TV_MOV_HORROR != '' THEN 'film/horror' END,
            CASE WHEN attr.MEDIA_TV_MOV_COMEDY IS NOT NULL AND attr.MEDIA_TV_MOV_COMEDY != '' THEN 'series/comedy' END,
            CASE WHEN attr.MEDIA_TV_MOV_DRAMA IS NOT NULL AND attr.MEDIA_TV_MOV_DRAMA != '' THEN 'film/drama' END,
            CASE WHEN attr.MEDIA_TV_MOV_DRAMA_MOVIES IS NOT NULL AND attr.MEDIA_TV_MOV_DRAMA_MOVIES != '' THEN 'film/drama' END,
            CASE WHEN attr.MEDIA_TV_MOV_ADVENTURE IS NOT NULL AND attr.MEDIA_TV_MOV_ADVENTURE != '' THEN 'film/adventure' END,
            CASE WHEN attr.MEDIA_TV_MOV_FAMILY_FILMS IS NOT NULL AND attr.MEDIA_TV_MOV_FAMILY_FILMS != '' THEN 'film/family' END,
            CASE WHEN attr.MEDIA_TV_MOV_ROMANTIC_COMEDY IS NOT NULL AND attr.MEDIA_TV_MOV_ROMANTIC_COMEDY != '' THEN 'film/romantic-comedy' END,
            CASE WHEN attr.MEDIA_TV_MOV_SCIFI IS NOT NULL AND attr.MEDIA_TV_MOV_SCIFI != '' THEN 'film/science-fiction' END,
            CASE WHEN attr.MEDIA_TV_MOV_THRILLER IS NOT NULL AND attr.MEDIA_TV_MOV_THRILLER != '' THEN 'film/thriller' END,
            CASE WHEN attr.MEDIA_TV_MOV_DOCU_FOREIGN IS NOT NULL AND attr.MEDIA_TV_MOV_DOCU_FOREIGN != '' THEN 'film/documentary' END,
            CASE WHEN attr.MEDIA_TV_MOV_CULT_CLASSIC IS NOT NULL AND attr.MEDIA_TV_MOV_CULT_CLASSIC != '' THEN 'film/cult-classic' END,
            CASE WHEN attr.MEDIA_TV_MOV_REALITY_TV IS NOT NULL AND attr.MEDIA_TV_MOV_REALITY_TV != '' THEN 'series/reality' END,
            CASE WHEN attr.MEDIA_TV_MOV_GAME_SHOWS IS NOT NULL AND attr.MEDIA_TV_MOV_GAME_SHOWS != '' THEN 'series/game-shows' END,
            CASE WHEN attr.MEDIA_TV_MOV_TV_NEWS IS NOT NULL AND attr.MEDIA_TV_MOV_TV_NEWS != '' THEN 'series/news' END,
            CASE WHEN attr.MEDIA_TV_MOV_TV_ANIMATION IS NOT NULL AND attr.MEDIA_TV_MOV_TV_ANIMATION != '' THEN 'series/animation' END,
            CASE WHEN attr.MEDIA_TV_MOV_TV_HISTORY IS NOT NULL AND attr.MEDIA_TV_MOV_TV_HISTORY != '' THEN 'series/history' END,
            CASE WHEN attr.MEDIA_TV_MOV_TV_HOW_TO IS NOT NULL AND attr.MEDIA_TV_MOV_TV_HOW_TO != '' THEN 'series/how-to' END,
            CASE WHEN attr.MEDIA_TV_MOV_STREAMING IS NOT NULL AND attr.MEDIA_TV_MOV_STREAMING != '' THEN 'streaming' END
        ) AS genres_array,
        
        -- Titles/Content Types (TITLES_WATCHED) - using all content preferences
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN attr.MEDIA_TV_MOV_HORROR IS NOT NULL AND attr.MEDIA_TV_MOV_HORROR != '' THEN 'horror' END,
            CASE WHEN attr.MEDIA_TV_MOV_COMEDY IS NOT NULL AND attr.MEDIA_TV_MOV_COMEDY != '' THEN 'comedy' END,
            CASE WHEN attr.MEDIA_TV_MOV_DRAMA IS NOT NULL AND attr.MEDIA_TV_MOV_DRAMA != '' THEN 'drama' END,
            CASE WHEN attr.MEDIA_TV_MOV_ADVENTURE IS NOT NULL AND attr.MEDIA_TV_MOV_ADVENTURE != '' THEN 'adventure' END,
            CASE WHEN attr.MEDIA_TV_MOV_FAMILY_FILMS IS NOT NULL AND attr.MEDIA_TV_MOV_FAMILY_FILMS != '' THEN 'family-films' END,
            CASE WHEN attr.MEDIA_TV_MOV_SCIFI IS NOT NULL AND attr.MEDIA_TV_MOV_SCIFI != '' THEN 'science-fiction' END,
            CASE WHEN attr.MEDIA_TV_MOV_THRILLER IS NOT NULL AND attr.MEDIA_TV_MOV_THRILLER != '' THEN 'thriller' END,
            CASE WHEN attr.MEDIA_TV_MOV_REALITY_TV IS NOT NULL AND attr.MEDIA_TV_MOV_REALITY_TV != '' THEN 'reality-tv' END,
            CASE WHEN attr.MEDIA_TV_MOV_GAME_SHOWS IS NOT NULL AND attr.MEDIA_TV_MOV_GAME_SHOWS != '' THEN 'game-shows' END,
            CASE WHEN attr.MEDIA_TV_MOV_TV_NEWS IS NOT NULL AND attr.MEDIA_TV_MOV_TV_NEWS != '' THEN 'tv-news' END,
            CASE WHEN attr.MEDIA_TV_MOV_TV_ANIMATION IS NOT NULL AND attr.MEDIA_TV_MOV_TV_ANIMATION != '' THEN 'animation' END,
            CASE WHEN attr.MEDIA_TV_MOV_HBO_WATCHER IS NOT NULL AND attr.MEDIA_TV_MOV_HBO_WATCHER != '' THEN 'hbo-content' END,
            CASE WHEN attr.MEDIA_TV_MOV_REDBOX IS NOT NULL AND attr.MEDIA_TV_MOV_REDBOX != '' THEN 'redbox' END,
            CASE WHEN attr.MEDIA_TV_MOV_OPRAH_FAN IS NOT NULL AND attr.MEDIA_TV_MOV_OPRAH_FAN != '' THEN 'oprah' END,
            CASE WHEN attr.MEDIA_TV_MOV_TOP_CHEF IS NOT NULL AND attr.MEDIA_TV_MOV_TOP_CHEF != '' THEN 'top-chef' END,
            CASE WHEN attr.MEDIA_TV_MOV_DISCOVERY_HISTORY IS NOT NULL AND attr.MEDIA_TV_MOV_DISCOVERY_HISTORY != '' THEN 'discovery-history' END,
            CASE WHEN attr.MEDIA_TV_MOV_COLLEGE_BASKETBALL IS NOT NULL AND attr.MEDIA_TV_MOV_COLLEGE_BASKETBALL != '' THEN 'college-basketball' END,
            CASE WHEN attr.MEDIA_TV_MOV_COLLEGE_FOOTBALL IS NOT NULL AND attr.MEDIA_TV_MOV_COLLEGE_FOOTBALL != '' THEN 'college-football' END,
            CASE WHEN attr.MEDIA_TV_MOV_TENNIS IS NOT NULL AND attr.MEDIA_TV_MOV_TENNIS != '' THEN 'tennis' END,
            CASE WHEN attr.MEDIA_TV_MOV_OSCARS IS NOT NULL AND attr.MEDIA_TV_MOV_OSCARS != '' THEN 'oscars' END,
            CASE WHEN attr.MEDIA_TV_MOV_GRAMMY IS NOT NULL AND attr.MEDIA_TV_MOV_GRAMMY != '' THEN 'grammy' END,
            CASE WHEN attr.MEDIA_TV_MOV_SUMMER_OLYMPICS IS NOT NULL AND attr.MEDIA_TV_MOV_SUMMER_OLYMPICS != '' THEN 'summer-olympics' END,
            CASE WHEN attr.MEDIA_TV_MOV_WINTER_OLYMPICS IS NOT NULL AND attr.MEDIA_TV_MOV_WINTER_OLYMPICS != '' THEN 'winter-olympics' END,
            CASE WHEN attr.MEDIA_TV_MOV_MOVIE_OPENING IS NOT NULL AND attr.MEDIA_TV_MOV_MOVIE_OPENING != '' THEN 'movie-openings' END,
            CASE WHEN attr.MEDIA_TV_MOV_FREQUENT_MOVIE IS NOT NULL AND attr.MEDIA_TV_MOV_FREQUENT_MOVIE != '' THEN 'frequent-movie-goer' END,
            CASE WHEN attr.MEDIA_TV_MOV_FEMALE_TV IS NOT NULL AND attr.MEDIA_TV_MOV_FEMALE_TV != '' THEN 'female-focused-tv' END,
            CASE WHEN attr.MEDIA_TV_MOV_GUY_SHOWS IS NOT NULL AND attr.MEDIA_TV_MOV_GUY_SHOWS != '' THEN 'male-focused-tv' END
        ) AS titles_array,
        
        -- Music Genres (for GENRES_WATCHED - adding music as a genre category)
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN attr.MEDIA_MUSIC_80S IS NOT NULL AND attr.MEDIA_MUSIC_80S != '' THEN 'music/80s' END,
            CASE WHEN attr.MEDIA_MUSIC_ALTERNATIVE IS NOT NULL AND attr.MEDIA_MUSIC_ALTERNATIVE != '' THEN 'music/alternative' END,
            CASE WHEN attr.MEDIA_MUSIC_CHRISTIAN IS NOT NULL AND attr.MEDIA_MUSIC_CHRISTIAN != '' THEN 'music/christian' END,
            CASE WHEN attr.MEDIA_MUSIC_CLASSICAL IS NOT NULL AND attr.MEDIA_MUSIC_CLASSICAL != '' THEN 'music/classical' END,
            CASE WHEN attr.MEDIA_MUSIC_COUNTRY IS NOT NULL AND attr.MEDIA_MUSIC_COUNTRY != '' THEN 'music/country' END,
            CASE WHEN attr.MEDIA_MUSIC_HIP_HOP IS NOT NULL AND attr.MEDIA_MUSIC_HIP_HOP != '' THEN 'music/hip-hop' END,
            CASE WHEN attr.MEDIA_MUSIC_JAZZ IS NOT NULL AND attr.MEDIA_MUSIC_JAZZ != '' THEN 'music/jazz' END,
            CASE WHEN attr.MEDIA_MUSIC_OLDIES IS NOT NULL AND attr.MEDIA_MUSIC_OLDIES != '' THEN 'music/oldies' END,
            CASE WHEN attr.MEDIA_MUSIC_POP IS NOT NULL AND attr.MEDIA_MUSIC_POP != '' THEN 'music/pop' END,
            CASE WHEN attr.MEDIA_MUSIC_ROCK IS NOT NULL AND attr.MEDIA_MUSIC_ROCK != '' THEN 'music/rock' END,
            CASE WHEN attr.MEDIA_MUSIC_GENERAL IS NOT NULL AND attr.MEDIA_MUSIC_GENERAL != '' THEN 'music/general' END
        ) AS music_genres_array
        
    FROM {{ ref('v_akkio_attributes_latest') }} attr
    WHERE attr.PARTITION_DATE IS NOT NULL
),

media_flattened AS (
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        'app_services' AS attr_type,
        value AS attr_value
    FROM media_values,
    LATERAL FLATTEN(INPUT => app_services_array)
    
    UNION ALL
    
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        'networks' AS attr_type,
        value AS attr_value
    FROM media_values,
    LATERAL FLATTEN(INPUT => networks_array)
    
    UNION ALL
    
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        'input_devices' AS attr_type,
        value AS attr_value
    FROM media_values,
    LATERAL FLATTEN(INPUT => audio_array)
    
    UNION ALL
    
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        'genres' AS attr_type,
        value AS attr_value
    FROM media_values,
    LATERAL FLATTEN(INPUT => genres_array)
    
    UNION ALL
    
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        'genres' AS attr_type,
        value AS attr_value
    FROM media_values,
    LATERAL FLATTEN(INPUT => music_genres_array)
    
    UNION ALL
    
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        'titles' AS attr_type,
        value AS attr_value
    FROM media_values,
    LATERAL FLATTEN(INPUT => titles_array)
),

media_aggregated AS (
    SELECT
        AKKIO_ID,
        PARTITION_DATE,
        attr_type,
        attr_value,
        COUNT(*) AS cnt
    FROM media_flattened
    GROUP BY AKKIO_ID, PARTITION_DATE, attr_type, attr_value
)

SELECT
    AKKIO_ID,
    PARTITION_DATE,
    
    -- Build OBJECT columns using OBJECT_AGG with WHERE clause filtering
    (SELECT OBJECT_AGG(attr_value, cnt) WITHIN GROUP (ORDER BY attr_value)
     FROM media_aggregated m2
     WHERE m2.AKKIO_ID = m1.AKKIO_ID
       AND m2.PARTITION_DATE = m1.PARTITION_DATE
       AND m2.attr_type = 'app_services') AS APP_SERVICES_USED,
    
    (SELECT OBJECT_AGG(attr_value, cnt) WITHIN GROUP (ORDER BY attr_value)
     FROM media_aggregated m2
     WHERE m2.AKKIO_ID = m1.AKKIO_ID
       AND m2.PARTITION_DATE = m1.PARTITION_DATE
       AND m2.attr_type = 'networks') AS NETWORKS_WATCHED,
    
    (SELECT OBJECT_AGG(attr_value, cnt) WITHIN GROUP (ORDER BY attr_value)
     FROM media_aggregated m2
     WHERE m2.AKKIO_ID = m1.AKKIO_ID
       AND m2.PARTITION_DATE = m1.PARTITION_DATE
       AND m2.attr_type = 'input_devices') AS INPUT_DEVICES_USED,
    
    (SELECT OBJECT_AGG(attr_value, cnt) WITHIN GROUP (ORDER BY attr_value)
     FROM media_aggregated m2
     WHERE m2.AKKIO_ID = m1.AKKIO_ID
       AND m2.PARTITION_DATE = m1.PARTITION_DATE
       AND m2.attr_type = 'genres') AS GENRES_WATCHED,
    
    -- TITLES_WATCHED: Use titles array which includes all content preferences
    (SELECT OBJECT_AGG(attr_value, cnt) WITHIN GROUP (ORDER BY attr_value)
     FROM media_aggregated m2
     WHERE m2.AKKIO_ID = m1.AKKIO_ID
       AND m2.PARTITION_DATE = m1.PARTITION_DATE
       AND m2.attr_type = 'titles') AS TITLES_WATCHED,
    
    -- Weight field (INSCAPE_WEIGHT equivalent)
    1 AS INSCAPE_WEIGHT

FROM (SELECT DISTINCT AKKIO_ID, PARTITION_DATE FROM media_aggregated) m1
WHERE EXISTS (
    SELECT 1 FROM media_aggregated m2
    WHERE m2.AKKIO_ID = m1.AKKIO_ID
      AND m2.PARTITION_DATE = m1.PARTITION_DATE
)
