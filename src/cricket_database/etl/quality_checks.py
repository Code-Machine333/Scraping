"""Data quality checking components."""

import logging
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, date
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..database import get_session
from ..models import Team, Player, Match, Inning, BallByBall, PlayerMatchStats

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Data quality checking component."""
    
    def __init__(self, enable_checks: bool = True):
        self.enable_checks = enable_checks
    
    async def check_data_quality(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks."""
        if not self.enable_checks:
            return {"status": "disabled"}
        
        logger.info("Running data quality checks")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        with get_session() as session:
            # Check teams
            results["checks"]["teams"] = await self._check_teams_quality(session)
            
            # Check players
            results["checks"]["players"] = await self._check_players_quality(session)
            
            # Check matches
            results["checks"]["matches"] = await self._check_matches_quality(session)
            
            # Check innings
            results["checks"]["innings"] = await self._check_innings_quality(session)
            
            # Check ball-by-ball
            results["checks"]["ball_by_ball"] = await self._check_ball_by_ball_quality(session)
            
            # Check player stats
            results["checks"]["player_stats"] = await self._check_player_stats_quality(session)
            
            # Check referential integrity
            results["checks"]["referential_integrity"] = await self._check_referential_integrity(session)
        
        # Calculate overall quality score
        results["overall_score"] = self._calculate_quality_score(results["checks"])
        
        logger.info(f"Data quality check completed. Overall score: {results['overall_score']}")
        return results
    
    async def _check_teams_quality(self, session: Session) -> Dict[str, Any]:
        """Check teams data quality."""
        issues = []
        
        # Check for duplicate team names
        duplicate_names = session.execute(
            select(Team.name, func.count(Team.id))
            .group_by(Team.name)
            .having(func.count(Team.id) > 1)
        ).fetchall()
        
        if duplicate_names:
            issues.append({
                "type": "duplicate_names",
                "count": len(duplicate_names),
                "details": [{"name": name, "count": count} for name, count in duplicate_names]
            })
        
        # Check for missing required fields
        missing_names = session.execute(
            select(func.count(Team.id))
            .where((Team.name == "") | (Team.name.is_(None)))
        ).scalar()
        
        if missing_names > 0:
            issues.append({
                "type": "missing_names",
                "count": missing_names
            })
        
        # Check for invalid countries
        invalid_countries = session.execute(
            select(func.count(Team.id))
            .where((Team.country == "") | (Team.country.is_(None)))
        ).scalar()
        
        if invalid_countries > 0:
            issues.append({
                "type": "invalid_countries",
                "count": invalid_countries
            })
        
        return {
            "total_teams": session.execute(select(func.count(Team.id))).scalar(),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    async def _check_players_quality(self, session: Session) -> Dict[str, Any]:
        """Check players data quality."""
        issues = []
        
        # Check for duplicate players (same name and team)
        duplicate_players = session.execute(
            select(Player.name, Player.team_id, func.count(Player.id))
            .group_by(Player.name, Player.team_id)
            .having(func.count(Player.id) > 1)
        ).fetchall()
        
        if duplicate_players:
            issues.append({
                "type": "duplicate_players",
                "count": len(duplicate_players),
                "details": [{"name": name, "team_id": team_id, "count": count} 
                           for name, team_id, count in duplicate_players]
            })
        
        # Check for missing required fields
        missing_names = session.execute(
            select(func.count(Player.id))
            .where((Player.name == "") | (Player.name.is_(None)))
        ).scalar()
        
        if missing_names > 0:
            issues.append({
                "type": "missing_names",
                "count": missing_names
            })
        
        # Check for invalid team references
        invalid_teams = session.execute(
            select(func.count(Player.id))
            .where(Player.team_id.is_(None))
        ).scalar()
        
        if invalid_teams > 0:
            issues.append({
                "type": "invalid_team_references",
                "count": invalid_teams
            })
        
        # Check for future birth dates
        future_birth_dates = session.execute(
            select(func.count(Player.id))
            .where(Player.date_of_birth > date.today())
        ).scalar()
        
        if future_birth_dates > 0:
            issues.append({
                "type": "future_birth_dates",
                "count": future_birth_dates
            })
        
        return {
            "total_players": session.execute(select(func.count(Player.id))).scalar(),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    async def _check_matches_quality(self, session: Session) -> Dict[str, Any]:
        """Check matches data quality."""
        issues = []
        
        # Check for duplicate matches (same teams and date)
        duplicate_matches = session.execute(
            select(Match.home_team_id, Match.away_team_id, Match.match_date, func.count(Match.id))
            .group_by(Match.home_team_id, Match.away_team_id, Match.match_date)
            .having(func.count(Match.id) > 1)
        ).fetchall()
        
        if duplicate_matches:
            issues.append({
                "type": "duplicate_matches",
                "count": len(duplicate_matches),
                "details": [{"home_team_id": home, "away_team_id": away, "date": date, "count": count}
                           for home, away, date, count in duplicate_matches]
            })
        
        # Check for future matches with completed status
        future_completed = session.execute(
            select(func.count(Match.id))
            .where((Match.match_date > date.today()) & (Match.status == "completed"))
        ).scalar()
        
        if future_completed > 0:
            issues.append({
                "type": "future_completed_matches",
                "count": future_completed
            })
        
        # Check for matches with same home and away team
        same_team_matches = session.execute(
            select(func.count(Match.id))
            .where(Match.home_team_id == Match.away_team_id)
        ).scalar()
        
        if same_team_matches > 0:
            issues.append({
                "type": "same_team_matches",
                "count": same_team_matches
            })
        
        return {
            "total_matches": session.execute(select(func.count(Match.id))).scalar(),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    async def _check_innings_quality(self, session: Session) -> Dict[str, Any]:
        """Check innings data quality."""
        issues = []
        
        # Check for duplicate innings (same match and inning number)
        duplicate_innings = session.execute(
            select(Inning.match_id, Inning.inning_number, func.count(Inning.id))
            .group_by(Inning.match_id, Inning.inning_number)
            .having(func.count(Inning.id) > 1)
        ).fetchall()
        
        if duplicate_innings:
            issues.append({
                "type": "duplicate_innings",
                "count": len(duplicate_innings),
                "details": [{"match_id": match_id, "inning_number": inning_num, "count": count}
                           for match_id, inning_num, count in duplicate_innings]
            })
        
        # Check for invalid scores
        invalid_scores = session.execute(
            select(func.count(Inning.id))
            .where((Inning.runs_scored < 0) | (Inning.wickets_lost < 0) | (Inning.wickets_lost > 10))
        ).scalar()
        
        if invalid_scores > 0:
            issues.append({
                "type": "invalid_scores",
                "count": invalid_scores
            })
        
        # Check for invalid overs
        invalid_overs = session.execute(
            select(func.count(Inning.id))
            .where((Inning.overs_bowled < 0) | (Inning.balls_bowled < 0) | (Inning.balls_bowled > 5))
        ).scalar()
        
        if invalid_overs > 0:
            issues.append({
                "type": "invalid_overs",
                "count": invalid_overs
            })
        
        return {
            "total_innings": session.execute(select(func.count(Inning.id))).scalar(),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    async def _check_ball_by_ball_quality(self, session: Session) -> Dict[str, Any]:
        """Check ball-by-ball data quality."""
        issues = []
        
        # Check for duplicate balls (same inning, over, and ball number)
        duplicate_balls = session.execute(
            select(BallByBall.inning_id, BallByBall.over_number, BallByBall.ball_number, func.count(BallByBall.id))
            .group_by(BallByBall.inning_id, BallByBall.over_number, BallByBall.ball_number)
            .having(func.count(BallByBall.id) > 1)
        ).fetchall()
        
        if duplicate_balls:
            issues.append({
                "type": "duplicate_balls",
                "count": len(duplicate_balls),
                "details": [{"inning_id": inning_id, "over": over, "ball": ball, "count": count}
                           for inning_id, over, ball, count in duplicate_balls]
            })
        
        # Check for invalid runs
        invalid_runs = session.execute(
            select(func.count(BallByBall.id))
            .where((BallByBall.runs_scored < 0) | (BallByBall.runs_scored > 6))
        ).scalar()
        
        if invalid_runs > 0:
            issues.append({
                "type": "invalid_runs",
                "count": invalid_runs
            })
        
        # Check for inconsistent boundary flags
        inconsistent_boundaries = session.execute(
            select(func.count(BallByBall.id))
            .where(
                ((BallByBall.is_six == True) & (BallByBall.runs_scored != 6)) |
                ((BallByBall.is_four == True) & (BallByBall.runs_scored != 4))
            )
        ).scalar()
        
        if inconsistent_boundaries > 0:
            issues.append({
                "type": "inconsistent_boundary_flags",
                "count": inconsistent_boundaries
            })
        
        return {
            "total_balls": session.execute(select(func.count(BallByBall.id))).scalar(),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    async def _check_player_stats_quality(self, session: Session) -> Dict[str, Any]:
        """Check player statistics data quality."""
        issues = []
        
        # Check for duplicate stats (same player and match)
        duplicate_stats = session.execute(
            select(PlayerMatchStats.player_id, PlayerMatchStats.match_id, func.count(PlayerMatchStats.id))
            .group_by(PlayerMatchStats.player_id, PlayerMatchStats.match_id)
            .having(func.count(PlayerMatchStats.id) > 1)
        ).fetchall()
        
        if duplicate_stats:
            issues.append({
                "type": "duplicate_stats",
                "count": len(duplicate_stats),
                "details": [{"player_id": player_id, "match_id": match_id, "count": count}
                           for player_id, match_id, count in duplicate_stats]
            })
        
        # Check for invalid statistics
        invalid_stats = session.execute(
            select(func.count(PlayerMatchStats.id))
            .where(
                (PlayerMatchStats.runs_scored < 0) |
                (PlayerMatchStats.balls_faced < 0) |
                (PlayerMatchStats.wickets_taken < 0) |
                (PlayerMatchStats.runs_conceded < 0)
            )
        ).scalar()
        
        if invalid_stats > 0:
            issues.append({
                "type": "invalid_statistics",
                "count": invalid_stats
            })
        
        return {
            "total_stats": session.execute(select(func.count(PlayerMatchStats.id))).scalar(),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    async def _check_referential_integrity(self, session: Session) -> Dict[str, Any]:
        """Check referential integrity between tables."""
        issues = []
        
        # Check for players with invalid team references
        invalid_player_teams = session.execute(
            select(func.count(Player.id))
            .where(~Player.team_id.in_(select(Team.id)))
        ).scalar()
        
        if invalid_player_teams > 0:
            issues.append({
                "type": "invalid_player_team_references",
                "count": invalid_player_teams
            })
        
        # Check for matches with invalid team references
        invalid_match_teams = session.execute(
            select(func.count(Match.id))
            .where(
                ~Match.home_team_id.in_(select(Team.id)) |
                ~Match.away_team_id.in_(select(Team.id))
            )
        ).scalar()
        
        if invalid_match_teams > 0:
            issues.append({
                "type": "invalid_match_team_references",
                "count": invalid_match_teams
            })
        
        # Check for innings with invalid match references
        invalid_inning_matches = session.execute(
            select(func.count(Inning.id))
            .where(~Inning.match_id.in_(select(Match.id)))
        ).scalar()
        
        if invalid_inning_matches > 0:
            issues.append({
                "type": "invalid_inning_match_references",
                "count": invalid_inning_matches
            })
        
        # Check for ball-by-ball with invalid inning references
        invalid_ball_innings = session.execute(
            select(func.count(BallByBall.id))
            .where(~BallByBall.inning_id.in_(select(Inning.id)))
        ).scalar()
        
        if invalid_ball_innings > 0:
            issues.append({
                "type": "invalid_ball_inning_references",
                "count": invalid_ball_innings
            })
        
        return {
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 15)
        }
    
    def _calculate_quality_score(self, checks: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        if not checks:
            return 0.0
        
        scores = []
        for check_name, check_result in checks.items():
            if isinstance(check_result, dict) and "quality_score" in check_result:
                scores.append(check_result["quality_score"])
        
        if not scores:
            return 0.0
        
        return round(sum(scores) / len(scores), 2)
    
    async def check_duplicates(self, table_name: str, fields: List[str]) -> List[Dict[str, Any]]:
        """Check for duplicate records in a specific table."""
        if not self.enable_checks:
            return []
        
        logger.info(f"Checking duplicates in {table_name} for fields: {fields}")
        
        # This would need to be implemented based on the specific table
        # For now, return empty list
        return []
    
    async def check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness and identify stale records."""
        if not self.enable_checks:
            return {"status": "disabled"}
        
        logger.info("Checking data freshness")
        
        with get_session() as session:
            # Check for old matches without recent updates
            old_matches = session.execute(
                select(func.count(Match.id))
                .where(Match.updated_at < datetime.now() - timedelta(days=30))
            ).scalar()
            
            # Check for players without recent activity
            inactive_players = session.execute(
                select(func.count(Player.id))
                .where(Player.updated_at < datetime.now() - timedelta(days=90))
            ).scalar()
            
            return {
                "old_matches": old_matches,
                "inactive_players": inactive_players,
                "last_check": datetime.now().isoformat()
            }
