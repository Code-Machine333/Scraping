"""Database loading components with idempotent upserts."""

import logging
from typing import Any, Dict, List, Optional, Union
from sqlalchemy import select, update, insert
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..database import get_session
from ..models import (
    Team, Player, Match, Inning, BallByBall, PlayerMatchStats, PlayerCareerStats
)
from ..schemas import (
    TeamCreate, PlayerCreate, MatchCreate, InningCreate, BallByBallCreate,
    PlayerMatchStatsCreate
)

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Database loader with idempotent upsert functionality."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    async def load_teams(self, teams_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load teams data with idempotent upserts."""
        logger.info(f"Loading {len(teams_data)} teams")
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        with get_session() as session:
            for team_data in teams_data:
                try:
                    result = await self._upsert_team(session, team_data)
                    if result == "inserted":
                        stats["inserted"] += 1
                    elif result == "updated":
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Failed to load team {team_data.get('name', 'Unknown')}: {e}")
                    stats["errors"] += 1
            
            session.commit()
        
        logger.info(f"Teams loaded: {stats}")
        return stats
    
    async def load_players(self, players_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load players data with idempotent upserts."""
        logger.info(f"Loading {len(players_data)} players")
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        with get_session() as session:
            for player_data in players_data:
                try:
                    result = await self._upsert_player(session, player_data)
                    if result == "inserted":
                        stats["inserted"] += 1
                    elif result == "updated":
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Failed to load player {player_data.get('name', 'Unknown')}: {e}")
                    stats["errors"] += 1
            
            session.commit()
        
        logger.info(f"Players loaded: {stats}")
        return stats
    
    async def load_matches(self, matches_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load matches data with idempotent upserts."""
        logger.info(f"Loading {len(matches_data)} matches")
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        with get_session() as session:
            for match_data in matches_data:
                try:
                    result = await self._upsert_match(session, match_data)
                    if result == "inserted":
                        stats["inserted"] += 1
                    elif result == "updated":
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Failed to load match: {e}")
                    stats["errors"] += 1
            
            session.commit()
        
        logger.info(f"Matches loaded: {stats}")
        return stats
    
    async def load_innings(self, innings_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load innings data with idempotent upserts."""
        logger.info(f"Loading {len(innings_data)} innings")
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        with get_session() as session:
            for inning_data in innings_data:
                try:
                    result = await self._upsert_inning(session, inning_data)
                    if result == "inserted":
                        stats["inserted"] += 1
                    elif result == "updated":
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Failed to load inning: {e}")
                    stats["errors"] += 1
            
            session.commit()
        
        logger.info(f"Innings loaded: {stats}")
        return stats
    
    async def load_ball_by_ball(self, balls_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load ball-by-ball data with idempotent upserts."""
        logger.info(f"Loading {len(balls_data)} ball-by-ball records")
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        with get_session() as session:
            for ball_data in balls_data:
                try:
                    result = await self._upsert_ball_by_ball(session, ball_data)
                    if result == "inserted":
                        stats["inserted"] += 1
                    elif result == "updated":
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Failed to load ball-by-ball record: {e}")
                    stats["errors"] += 1
            
            session.commit()
        
        logger.info(f"Ball-by-ball records loaded: {stats}")
        return stats
    
    async def load_player_stats(self, stats_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Load player statistics data with idempotent upserts."""
        logger.info(f"Loading {len(stats_data)} player statistics records")
        
        stats = {"inserted": 0, "updated": 0, "errors": 0}
        
        with get_session() as session:
            for stat_data in stats_data:
                try:
                    result = await self._upsert_player_stats(session, stat_data)
                    if result == "inserted":
                        stats["inserted"] += 1
                    elif result == "updated":
                        stats["updated"] += 1
                except Exception as e:
                    logger.error(f"Failed to load player stats: {e}")
                    stats["errors"] += 1
            
            session.commit()
        
        logger.info(f"Player statistics loaded: {stats}")
        return stats
    
    async def _upsert_team(self, session: Session, team_data: Dict[str, Any]) -> str:
        """Upsert team data."""
        # Try to find existing team by name or short_name
        existing_team = session.execute(
            select(Team).where(
                (Team.name == team_data["name"]) | 
                (Team.short_name == team_data["short_name"])
            )
        ).scalar_one_or_none()
        
        if existing_team:
            # Update existing team
            for key, value in team_data.items():
                if hasattr(existing_team, key):
                    setattr(existing_team, key, value)
            return "updated"
        else:
            # Insert new team
            team = Team(**team_data)
            session.add(team)
            return "inserted"
    
    async def _upsert_player(self, session: Session, player_data: Dict[str, Any]) -> str:
        """Upsert player data."""
        # Try to find existing player by name and team_id
        existing_player = session.execute(
            select(Player).where(
                (Player.name == player_data["name"]) & 
                (Player.team_id == player_data["team_id"])
            )
        ).scalar_one_or_none()
        
        if existing_player:
            # Update existing player
            for key, value in player_data.items():
                if hasattr(existing_player, key):
                    setattr(existing_player, key, value)
            return "updated"
        else:
            # Insert new player
            player = Player(**player_data)
            session.add(player)
            return "inserted"
    
    async def _upsert_match(self, session: Session, match_data: Dict[str, Any]) -> str:
        """Upsert match data."""
        # Try to find existing match by teams and date
        existing_match = session.execute(
            select(Match).where(
                (Match.home_team_id == match_data["home_team_id"]) &
                (Match.away_team_id == match_data["away_team_id"]) &
                (Match.match_date == match_data["match_date"])
            )
        ).scalar_one_or_none()
        
        if existing_match:
            # Update existing match
            for key, value in match_data.items():
                if hasattr(existing_match, key):
                    setattr(existing_match, key, value)
            return "updated"
        else:
            # Insert new match
            match = Match(**match_data)
            session.add(match)
            return "inserted"
    
    async def _upsert_inning(self, session: Session, inning_data: Dict[str, Any]) -> str:
        """Upsert inning data."""
        # Try to find existing inning by match_id and inning_number
        existing_inning = session.execute(
            select(Inning).where(
                (Inning.match_id == inning_data["match_id"]) &
                (Inning.inning_number == inning_data["inning_number"])
            )
        ).scalar_one_or_none()
        
        if existing_inning:
            # Update existing inning
            for key, value in inning_data.items():
                if hasattr(existing_inning, key):
                    setattr(existing_inning, key, value)
            return "updated"
        else:
            # Insert new inning
            inning = Inning(**inning_data)
            session.add(inning)
            return "inserted"
    
    async def _upsert_ball_by_ball(self, session: Session, ball_data: Dict[str, Any]) -> str:
        """Upsert ball-by-ball data."""
        # Try to find existing ball by inning_id, over_number, and ball_number
        existing_ball = session.execute(
            select(BallByBall).where(
                (BallByBall.inning_id == ball_data["inning_id"]) &
                (BallByBall.over_number == ball_data["over_number"]) &
                (BallByBall.ball_number == ball_data["ball_number"])
            )
        ).scalar_one_or_none()
        
        if existing_ball:
            # Update existing ball
            for key, value in ball_data.items():
                if hasattr(existing_ball, key):
                    setattr(existing_ball, key, value)
            return "updated"
        else:
            # Insert new ball
            ball = BallByBall(**ball_data)
            session.add(ball)
            return "inserted"
    
    async def _upsert_player_stats(self, session: Session, stats_data: Dict[str, Any]) -> str:
        """Upsert player statistics data."""
        # Try to find existing stats by player_id and match_id
        existing_stats = session.execute(
            select(PlayerMatchStats).where(
                (PlayerMatchStats.player_id == stats_data["player_id"]) &
                (PlayerMatchStats.match_id == stats_data["match_id"])
            )
        ).scalar_one_or_none()
        
        if existing_stats:
            # Update existing stats
            for key, value in stats_data.items():
                if hasattr(existing_stats, key):
                    setattr(existing_stats, key, value)
            return "updated"
        else:
            # Insert new stats
            stats = PlayerMatchStats(**stats_data)
            session.add(stats)
            return "inserted"
    
    async def load_all_data(self, data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, int]]:
        """Load all data types with proper dependency order."""
        logger.info("Loading all data with dependency order")
        
        results = {}
        
        # Load in dependency order
        if "teams" in data:
            results["teams"] = await self.load_teams(data["teams"])
        
        if "players" in data:
            results["players"] = await self.load_players(data["players"])
        
        if "matches" in data:
            results["matches"] = await self.load_matches(data["matches"])
        
        if "innings" in data:
            results["innings"] = await self.load_innings(data["innings"])
        
        if "ball_by_ball" in data:
            results["ball_by_ball"] = await self.load_ball_by_ball(data["ball_by_ball"])
        
        if "player_stats" in data:
            results["player_stats"] = await self.load_player_stats(data["player_stats"])
        
        logger.info("All data loaded successfully")
        return results
