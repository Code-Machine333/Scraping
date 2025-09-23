-- Duplicate Detection Queries for Cricket Database
-- These queries identify potential duplicate records that need review

-- =============================================================================
-- 1. DUPLICATE MATCHES BY DATE, TEAMS, VENUE, FORMAT
-- =============================================================================

-- Find matches that appear to be duplicates based on key attributes
WITH match_duplicates AS (
    SELECT 
        m1.id as match_id_1,
        m2.id as match_id_2,
        m1.start_date,
        m1.format,
        v.name as venue_name,
        t1.name as team1_name,
        t2.name as team2_name,
        m1.source_match_key as source_key_1,
        m2.source_match_key as source_key_2,
        CONCAT(
            'Potential duplicate: ', 
            DATE_FORMAT(m1.start_date, '%Y-%m-%d'), 
            ' - ', 
            t1.name, ' vs ', t2.name,
            ' at ', v.name,
            ' (', m1.format, ')'
        ) as description
    FROM matches m1
    JOIN matches m2 ON m1.id < m2.id  -- Avoid self-comparison and duplicates
    JOIN venues v ON m1.venue_id = v.id
    JOIN match_teams mt1_1 ON m1.id = mt1_1.match_id AND mt1_1.is_home = 1
    JOIN match_teams mt1_2 ON m1.id = mt1_2.match_id AND mt1_2.is_home = 0
    JOIN match_teams mt2_1 ON m2.id = mt2_1.match_id AND mt2_1.is_home = 1
    JOIN match_teams mt2_2 ON m2.id = mt2_2.match_id AND mt2_2.is_home = 0
    JOIN teams t1 ON mt1_1.team_id = t1.id
    JOIN teams t2 ON mt1_2.team_id = t2.id
    WHERE 
        -- Same date (or within 1 day for potential rescheduled matches)
        ABS(DATEDIFF(m1.start_date, m2.start_date)) <= 1
        -- Same format
        AND m1.format = m2.format
        -- Same venue
        AND m1.venue_id = m2.venue_id
        -- Same teams (order doesn't matter)
        AND (
            (mt1_1.team_id = mt2_1.team_id AND mt1_2.team_id = mt2_2.team_id)
            OR (mt1_1.team_id = mt2_2.team_id AND mt1_2.team_id = mt2_1.team_id)
        )
)
SELECT 
    'duplicate_matches' as check_name,
    COUNT(*) as issue_count,
    'Potential duplicate matches found' as description
FROM match_duplicates;

-- Detailed view of duplicate matches (uncomment to see details)
/*
SELECT 
    match_id_1,
    match_id_2,
    start_date,
    format,
    venue_name,
    team1_name,
    team2_name,
    source_key_1,
    source_key_2,
    description
FROM match_duplicates
ORDER BY start_date DESC, venue_name;
*/

-- =============================================================================
-- 2. PLAYERS WITH SAME NAME BUT DIFFERENT DOB
-- =============================================================================

-- Find players with identical names but different birth dates
WITH player_name_duplicates AS (
    SELECT 
        p1.id as player_id_1,
        p2.id as player_id_2,
        p1.full_name,
        p1.born_date as dob_1,
        p2.born_date as dob_2,
        DATEDIFF(p1.born_date, p2.born_date) as dob_difference_days,
        CONCAT(
            'Same name, different DOB: ', 
            p1.full_name,
            ' (', DATE_FORMAT(p1.born_date, '%Y-%m-%d'), ' vs ', DATE_FORMAT(p2.born_date, '%Y-%m-%d'), ')'
        ) as description
    FROM players p1
    JOIN players p2 ON p1.id < p2.id  -- Avoid self-comparison and duplicates
    WHERE 
        -- Same normalized name
        p1.full_name = p2.full_name
        -- Both have birth dates
        AND p1.born_date IS NOT NULL 
        AND p2.born_date IS NOT NULL
        -- Different birth dates (more than 30 days apart to avoid data entry errors)
        AND ABS(DATEDIFF(p1.born_date, p2.born_date)) > 30
)
SELECT 
    'duplicate_player_names' as check_name,
    COUNT(*) as issue_count,
    'Players with same name but different birth dates' as description
FROM player_name_duplicates;

-- Detailed view of player name duplicates (uncomment to see details)
/*
SELECT 
    player_id_1,
    player_id_2,
    full_name,
    dob_1,
    dob_2,
    dob_difference_days,
    description
FROM player_name_duplicates
ORDER BY full_name, dob_1;
*/

-- =============================================================================
-- 3. TEAMS WITH SIMILAR NAMES (POTENTIAL DUPLICATES)
-- =============================================================================

-- Find teams with very similar names (potential duplicates or aliases)
WITH team_name_similarities AS (
    SELECT 
        t1.id as team_id_1,
        t2.id as team_id_2,
        t1.name as name_1,
        t2.name as name_2,
        c1.name as country_1,
        c2.name as country_2,
        CONCAT(
            'Similar team names: "', t1.name, '" vs "', t2.name, '"'
        ) as description
    FROM teams t1
    JOIN teams t2 ON t1.id < t2.id
    JOIN countries c1 ON t1.country_id = c1.id
    JOIN countries c2 ON t2.country_id = c2.id
    WHERE 
        -- Same country
        t1.country_id = t2.country_id
        -- Similar names (using SOUNDEX for phonetic similarity)
        AND (
            SOUNDEX(t1.name) = SOUNDEX(t2.name)
            OR t1.name LIKE CONCAT('%', SUBSTRING(t2.name, 1, 4), '%')
            OR t2.name LIKE CONCAT('%', SUBSTRING(t1.name, 1, 4), '%')
        )
        -- Not exactly the same (handled by unique constraint)
        AND t1.name != t2.name
)
SELECT 
    'similar_team_names' as check_name,
    COUNT(*) as issue_count,
    'Teams with similar names in same country' as description
FROM team_name_similarities;

-- =============================================================================
-- 4. VENUES WITH SIMILAR NAMES (POTENTIAL DUPLICATES)
-- =============================================================================

-- Find venues with very similar names in the same country
WITH venue_name_similarities AS (
    SELECT 
        v1.id as venue_id_1,
        v2.id as venue_id_2,
        v1.name as name_1,
        v2.name as name_2,
        c1.name as country_1,
        c2.name as country_2,
        CONCAT(
            'Similar venue names: "', v1.name, '" vs "', v2.name, '" in ', c1.name
        ) as description
    FROM venues v1
    JOIN venues v2 ON v1.id < v2.id
    JOIN countries c1 ON v1.country_id = c1.id
    JOIN countries c2 ON v2.country_id = c2.id
    WHERE 
        -- Same country
        v1.country_id = v2.country_id
        -- Similar names
        AND (
            SOUNDEX(v1.name) = SOUNDEX(v2.name)
            OR v1.name LIKE CONCAT('%', SUBSTRING(v2.name, 1, 5), '%')
            OR v2.name LIKE CONCAT('%', SUBSTRING(v1.name, 1, 5), '%')
        )
        -- Not exactly the same
        AND v1.name != v2.name
)
SELECT 
    'similar_venue_names' as check_name,
    COUNT(*) as issue_count,
    'Venues with similar names in same country' as description
FROM venue_name_similarities;

-- =============================================================================
-- 5. DUPLICATE SOURCE KEYS
-- =============================================================================

-- Find duplicate source keys (should be unique per entity type and source)
WITH duplicate_source_keys AS (
    SELECT 
        entity_type,
        source_id,
        source_key,
        COUNT(*) as key_count,
        GROUP_CONCAT(canonical_id ORDER BY canonical_id) as canonical_ids,
        CONCAT(
            'Duplicate source key: ', entity_type, ':', source_key, 
            ' maps to ', key_count, ' different canonical IDs'
        ) as description
    FROM source_keys
    GROUP BY entity_type, source_id, source_key
    HAVING COUNT(*) > 1
)
SELECT 
    'duplicate_source_keys' as check_name,
    COUNT(*) as issue_count,
    'Source keys that map to multiple canonical IDs' as description
FROM duplicate_source_keys;

-- =============================================================================
-- 6. PLAYERS WITH MULTIPLE ALIASES (POTENTIAL MERGE CANDIDATES)
-- =============================================================================

-- Find players that have multiple aliases (might be the same person)
WITH player_alias_counts AS (
    SELECT 
        p.id as player_id,
        p.full_name,
        p.born_date,
        COUNT(pa.id) as alias_count,
        GROUP_CONCAT(pa.alias_name ORDER BY pa.alias_name) as aliases,
        CONCAT(
            'Player with multiple aliases: ', p.full_name, 
            ' has ', COUNT(pa.id), ' aliases'
        ) as description
    FROM players p
    LEFT JOIN player_alias pa ON p.id = pa.player_id
    GROUP BY p.id, p.full_name, p.born_date
    HAVING COUNT(pa.id) > 2  -- More than 2 aliases is suspicious
)
SELECT 
    'players_multiple_aliases' as check_name,
    COUNT(*) as issue_count,
    'Players with more than 2 aliases' as description
FROM player_alias_counts;

-- =============================================================================
-- 7. TEAMS WITH MULTIPLE ALIASES (POTENTIAL MERGE CANDIDATES)
-- =============================================================================

-- Find teams that have multiple aliases
WITH team_alias_counts AS (
    SELECT 
        t.id as team_id,
        t.name,
        c.name as country,
        COUNT(ta.id) as alias_count,
        GROUP_CONCAT(ta.alias_name ORDER BY ta.alias_name) as aliases,
        CONCAT(
            'Team with multiple aliases: ', t.name, 
            ' (', c.name, ') has ', COUNT(ta.id), ' aliases'
        ) as description
    FROM teams t
    JOIN countries c ON t.country_id = c.id
    LEFT JOIN team_alias ta ON t.id = ta.team_id
    GROUP BY t.id, t.name, c.name
    HAVING COUNT(ta.id) > 2  -- More than 2 aliases is suspicious
)
SELECT 
    'teams_multiple_aliases' as check_name,
    COUNT(*) as issue_count,
    'Teams with more than 2 aliases' as description
FROM team_alias_counts;
