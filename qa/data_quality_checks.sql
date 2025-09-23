-- Data Quality Checks for Cricket Database
-- These queries identify data integrity issues that need attention

-- =============================================================================
-- 1. NULL CHECKS FOR CRITICAL FOREIGN KEYS
-- =============================================================================

-- Matches with missing venue
SELECT 
    'matches_missing_venue' as check_name,
    COUNT(*) as issue_count,
    'Matches without venue_id' as description
FROM matches 
WHERE venue_id IS NULL;

-- Matches with missing series
SELECT 
    'matches_missing_series' as check_name,
    COUNT(*) as issue_count,
    'Matches without series_id' as description
FROM matches 
WHERE series_id IS NULL;

-- Innings with missing teams
SELECT 
    'innings_missing_teams' as check_name,
    COUNT(*) as issue_count,
    'Innings without batting_team_id or bowling_team_id' as description
FROM innings 
WHERE batting_team_id IS NULL OR bowling_team_id IS NULL;

-- Batting entries with missing player
SELECT 
    'batting_missing_player' as check_name,
    COUNT(*) as issue_count,
    'Batting entries without player_id' as description
FROM batting_innings 
WHERE player_id IS NULL;

-- Bowling entries with missing player
SELECT 
    'bowling_missing_player' as check_name,
    COUNT(*) as issue_count,
    'Bowling entries without player_id' as description
FROM bowling_innings 
WHERE player_id IS NULL;

-- Deliveries with missing players
SELECT 
    'deliveries_missing_players' as check_name,
    COUNT(*) as issue_count,
    'Deliveries without striker_id, non_striker_id, or bowler_id' as description
FROM deliveries 
WHERE striker_id IS NULL OR non_striker_id IS NULL OR bowler_id IS NULL;

-- =============================================================================
-- 2. INNINGS TOTALS VS BATTING + EXTRAS VALIDATION
-- =============================================================================

-- Innings where declared total doesn't match batting + extras
WITH innings_totals AS (
    SELECT 
        i.id as innings_id,
        i.match_id,
        i.innings_no,
        i.runs as declared_runs,
        i.wickets as declared_wickets,
        COALESCE(SUM(bi.runs), 0) as batting_runs,
        COALESCE(SUM(bi.balls), 0) as batting_balls,
        COUNT(DISTINCT bi.player_id) as batsmen_count,
        -- Calculate extras from deliveries
        COALESCE(SUM(d.extras_bye + d.extras_legbye + d.extras_wide + d.extras_noball + d.extras_penalty), 0) as extras_runs
    FROM innings i
    LEFT JOIN batting_innings bi ON i.id = bi.innings_id
    LEFT JOIN deliveries d ON i.id = d.innings_id
    GROUP BY i.id, i.match_id, i.innings_no, i.runs, i.wickets
)
SELECT 
    'innings_total_mismatch' as check_name,
    COUNT(*) as issue_count,
    'Innings where declared runs != batting runs + extras' as description
FROM innings_totals
WHERE declared_runs != (batting_runs + extras_runs)
  AND declared_runs > 0;  -- Skip incomplete innings

-- =============================================================================
-- 3. BOWLING OVERS × 6 EQUALS DELIVERIES VALIDATION
-- =============================================================================

-- Bowling entries where overs × 6 doesn't match actual deliveries
WITH bowling_delivery_counts AS (
    SELECT 
        bi.id as bowling_innings_id,
        bi.player_id,
        bi.overs as declared_overs,
        bi.maidens,
        bi.runs as declared_runs,
        bi.wickets as declared_wickets,
        bi.wides,
        bi.no_balls,
        -- Count actual deliveries bowled by this player
        COUNT(d.id) as actual_deliveries,
        -- Calculate expected deliveries (overs × 6 + wides + noballs)
        (FLOOR(bi.overs) * 6 + ((bi.overs - FLOOR(bi.overs)) * 10)) as expected_deliveries
    FROM bowling_innings bi
    LEFT JOIN deliveries d ON bi.innings_id = d.innings_id AND bi.player_id = d.bowler_id
    GROUP BY bi.id, bi.player_id, bi.overs, bi.maidens, bi.runs, bi.wickets, bi.wides, bi.no_balls
)
SELECT 
    'bowling_overs_mismatch' as check_name,
    COUNT(*) as issue_count,
    'Bowling entries where overs calculation is inconsistent' as description
FROM bowling_delivery_counts
WHERE ABS(actual_deliveries - expected_deliveries) > 1;  -- Allow 1 delivery tolerance

-- =============================================================================
-- 4. MATCH DATE AND FORMAT CONSISTENCY
-- =============================================================================

-- Matches with invalid date ranges
SELECT 
    'invalid_date_ranges' as check_name,
    COUNT(*) as issue_count,
    'Matches where end_date is before start_date' as description
FROM matches 
WHERE end_date IS NOT NULL 
  AND start_date IS NOT NULL 
  AND end_date < start_date;

-- Matches with missing format
SELECT 
    'matches_missing_format' as check_name,
    COUNT(*) as issue_count,
    'Matches without format specified' as description
FROM matches 
WHERE format IS NULL OR format = '';

-- =============================================================================
-- 5. PLAYER DATA CONSISTENCY
-- =============================================================================

-- Players with missing birth dates
SELECT 
    'players_missing_dob' as check_name,
    COUNT(*) as issue_count,
    'Players without birth_date' as description
FROM players 
WHERE born_date IS NULL;

-- Players with invalid birth dates (future dates)
SELECT 
    'players_future_dob' as check_name,
    COUNT(*) as issue_count,
    'Players with birth_date in the future' as description
FROM players 
WHERE born_date > CURDATE();

-- =============================================================================
-- 6. TEAM DATA CONSISTENCY
-- =============================================================================

-- Teams with missing country
SELECT 
    'teams_missing_country' as check_name,
    COUNT(*) as issue_count,
    'Teams without country_id' as description
FROM teams 
WHERE country_id IS NULL;

-- =============================================================================
-- 7. VENUE DATA CONSISTENCY
-- =============================================================================

-- Venues with missing country
SELECT 
    'venues_missing_country' as check_name,
    COUNT(*) as issue_count,
    'Venues without country_id' as description
FROM venues 
WHERE country_id IS NULL;

-- =============================================================================
-- 8. SERIES AND SEASON CONSISTENCY
-- =============================================================================

-- Series with missing season
SELECT 
    'series_missing_season' as check_name,
    COUNT(*) as issue_count,
    'Series without season_id' as description
FROM series 
WHERE season_id IS NULL;

-- Seasons with invalid date ranges
SELECT 
    'seasons_invalid_dates' as check_name,
    COUNT(*) as issue_count,
    'Seasons where end_date is before start_date' as description
FROM seasons 
WHERE end_date IS NOT NULL 
  AND start_date IS NOT NULL 
  AND end_date < start_date;
