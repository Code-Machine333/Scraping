"""Data transformation and validation components."""

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

from pydantic import ValidationError

from ..schemas import (
    TeamCreate, PlayerCreate, MatchCreate, InningCreate, BallByBallCreate,
    PlayerMatchStatsCreate
)
from ..models import MatchType, MatchStatus, InningStatus

logger = logging.getLogger(__name__)


class DataValidator:
    """Data validation component."""
    
    def __init__(self, enable_validation: bool = True):
        self.enable_validation = enable_validation
    
    def validate_team_data(self, data: Dict[str, Any]) -> Optional[TeamCreate]:
        """Validate team data using Pydantic schema."""
        if not self.enable_validation:
            return data
        
        try:
            return TeamCreate(**data)
        except ValidationError as e:
            logger.warning(f"Team data validation failed: {e}")
            return None
    
    def validate_player_data(self, data: Dict[str, Any]) -> Optional[PlayerCreate]:
        """Validate player data using Pydantic schema."""
        if not self.enable_validation:
            return data
        
        try:
            return PlayerCreate(**data)
        except ValidationError as e:
            logger.warning(f"Player data validation failed: {e}")
            return None
    
    def validate_match_data(self, data: Dict[str, Any]) -> Optional[MatchCreate]:
        """Validate match data using Pydantic schema."""
        if not self.enable_validation:
            return data
        
        try:
            return MatchCreate(**data)
        except ValidationError as e:
            logger.warning(f"Match data validation failed: {e}")
            return None
    
    def validate_inning_data(self, data: Dict[str, Any]) -> Optional[InningCreate]:
        """Validate inning data using Pydantic schema."""
        if not self.enable_validation:
            return data
        
        try:
            return InningCreate(**data)
        except ValidationError as e:
            logger.warning(f"Inning data validation failed: {e}")
            return None
    
    def validate_ball_by_ball_data(self, data: Dict[str, Any]) -> Optional[BallByBallCreate]:
        """Validate ball-by-ball data using Pydantic schema."""
        if not self.enable_validation:
            return data
        
        try:
            return BallByBallCreate(**data)
        except ValidationError as e:
            logger.warning(f"Ball-by-ball data validation failed: {e}")
            return None
    
    def validate_player_stats_data(self, data: Dict[str, Any]) -> Optional[PlayerMatchStatsCreate]:
        """Validate player stats data using Pydantic schema."""
        if not self.enable_validation:
            return data
        
        try:
            return PlayerMatchStatsCreate(**data)
        except ValidationError as e:
            logger.warning(f"Player stats data validation failed: {e}")
            return None


class DataTransformer:
    """Data transformation component."""
    
    def __init__(self, validator: Optional[DataValidator] = None):
        self.validator = validator or DataValidator()
    
    def transform_teams(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw team data."""
        transformed_teams = []
        
        for team_data in raw_data:
            try:
                # Clean and standardize team data
                cleaned_data = self._clean_team_data(team_data)
                
                # Validate data
                validated_data = self.validator.validate_team_data(cleaned_data)
                if validated_data:
                    transformed_teams.append(validated_data.dict())
                
            except Exception as e:
                logger.warning(f"Failed to transform team data: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_teams)} teams")
        return transformed_teams
    
    def transform_players(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw player data."""
        transformed_players = []
        
        for player_data in raw_data:
            try:
                # Clean and standardize player data
                cleaned_data = self._clean_player_data(player_data)
                
                # Validate data
                validated_data = self.validator.validate_player_data(cleaned_data)
                if validated_data:
                    transformed_players.append(validated_data.dict())
                
            except Exception as e:
                logger.warning(f"Failed to transform player data: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_players)} players")
        return transformed_players
    
    def transform_matches(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw match data."""
        transformed_matches = []
        
        for match_data in raw_data:
            try:
                # Clean and standardize match data
                cleaned_data = self._clean_match_data(match_data)
                
                # Validate data
                validated_data = self.validator.validate_match_data(cleaned_data)
                if validated_data:
                    transformed_matches.append(validated_data.dict())
                
            except Exception as e:
                logger.warning(f"Failed to transform match data: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_matches)} matches")
        return transformed_matches
    
    def transform_innings(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw inning data."""
        transformed_innings = []
        
        for inning_data in raw_data:
            try:
                # Clean and standardize inning data
                cleaned_data = self._clean_inning_data(inning_data)
                
                # Validate data
                validated_data = self.validator.validate_inning_data(cleaned_data)
                if validated_data:
                    transformed_innings.append(validated_data.dict())
                
            except Exception as e:
                logger.warning(f"Failed to transform inning data: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_innings)} innings")
        return transformed_innings
    
    def transform_ball_by_ball(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw ball-by-ball data."""
        transformed_balls = []
        
        for ball_data in raw_data:
            try:
                # Clean and standardize ball data
                cleaned_data = self._clean_ball_by_ball_data(ball_data)
                
                # Validate data
                validated_data = self.validator.validate_ball_by_ball_data(cleaned_data)
                if validated_data:
                    transformed_balls.append(validated_data.dict())
                
            except Exception as e:
                logger.warning(f"Failed to transform ball-by-ball data: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_balls)} balls")
        return transformed_balls
    
    def _clean_team_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize team data."""
        cleaned = {}
        
        # Required fields
        cleaned["name"] = str(data.get("name", "")).strip()
        cleaned["short_name"] = str(data.get("short_name", "")).strip().upper()
        cleaned["country"] = str(data.get("country", "")).strip()
        
        # Optional fields
        if data.get("logo_url"):
            cleaned["logo_url"] = str(data["logo_url"]).strip()
        if data.get("website_url"):
            cleaned["website_url"] = str(data["website_url"]).strip()
        if data.get("description"):
            cleaned["description"] = str(data["description"]).strip()
        
        # Boolean fields
        cleaned["is_active"] = bool(data.get("is_active", True))
        cleaned["is_test_playing"] = bool(data.get("is_test_playing", False))
        cleaned["is_odi_playing"] = bool(data.get("is_odi_playing", True))
        cleaned["is_t20_playing"] = bool(data.get("is_t20_playing", True))
        
        return cleaned
    
    def _clean_player_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize player data."""
        cleaned = {}
        
        # Required fields
        cleaned["name"] = str(data.get("name", "")).strip()
        cleaned["team_id"] = int(data.get("team_id", 0))
        cleaned["nationality"] = str(data.get("nationality", "")).strip()
        cleaned["primary_role"] = str(data.get("primary_role", "Batsman")).strip()
        
        # Optional fields
        if data.get("full_name"):
            cleaned["full_name"] = str(data["full_name"]).strip()
        if data.get("date_of_birth"):
            cleaned["date_of_birth"] = data["date_of_birth"]
        if data.get("place_of_birth"):
            cleaned["place_of_birth"] = str(data["place_of_birth"]).strip()
        if data.get("height_cm"):
            cleaned["height_cm"] = int(data["height_cm"])
        if data.get("weight_kg"):
            cleaned["weight_kg"] = int(data["weight_kg"])
        if data.get("batting_style"):
            cleaned["batting_style"] = str(data["batting_style"]).strip()
        if data.get("bowling_style"):
            cleaned["bowling_style"] = str(data["bowling_style"]).strip()
        if data.get("secondary_role"):
            cleaned["secondary_role"] = str(data["secondary_role"]).strip()
        if data.get("debut_date"):
            cleaned["debut_date"] = data["debut_date"]
        if data.get("retirement_date"):
            cleaned["retirement_date"] = data["retirement_date"]
        if data.get("espn_id"):
            cleaned["espn_id"] = str(data["espn_id"]).strip()
        if data.get("cricinfo_id"):
            cleaned["cricinfo_id"] = str(data["cricinfo_id"]).strip()
        
        # Boolean fields
        cleaned["is_active"] = bool(data.get("is_active", True))
        
        return cleaned
    
    def _clean_match_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize match data."""
        cleaned = {}
        
        # Required fields
        cleaned["match_type"] = str(data.get("match_type", "odi")).lower()
        cleaned["status"] = str(data.get("status", "scheduled")).lower()
        cleaned["home_team_id"] = int(data.get("home_team_id", 0))
        cleaned["away_team_id"] = int(data.get("away_team_id", 0))
        cleaned["match_date"] = data.get("match_date")
        
        # Optional fields
        if data.get("start_time"):
            cleaned["start_time"] = data["start_time"]
        if data.get("end_time"):
            cleaned["end_time"] = data["end_time"]
        if data.get("venue_name"):
            cleaned["venue_name"] = str(data["venue_name"]).strip()
        if data.get("venue_city"):
            cleaned["venue_city"] = str(data["venue_city"]).strip()
        if data.get("venue_country"):
            cleaned["venue_country"] = str(data["venue_country"]).strip()
        if data.get("venue_capacity"):
            cleaned["venue_capacity"] = int(data["venue_capacity"])
        if data.get("series_name"):
            cleaned["series_name"] = str(data["series_name"]).strip()
        if data.get("series_type"):
            cleaned["series_type"] = str(data["series_type"]).strip()
        if data.get("match_number"):
            cleaned["match_number"] = int(data["match_number"])
        if data.get("total_matches_in_series"):
            cleaned["total_matches_in_series"] = int(data["total_matches_in_series"])
        if data.get("toss_winner_id"):
            cleaned["toss_winner_id"] = int(data["toss_winner_id"])
        if data.get("toss_decision"):
            cleaned["toss_decision"] = str(data["toss_decision"]).lower()
        if data.get("match_winner_id"):
            cleaned["match_winner_id"] = int(data["match_winner_id"])
        if data.get("win_margin"):
            cleaned["win_margin"] = str(data["win_margin"]).strip()
        if data.get("win_type"):
            cleaned["win_type"] = str(data["win_type"]).lower()
        if data.get("umpire_1"):
            cleaned["umpire_1"] = str(data["umpire_1"]).strip()
        if data.get("umpire_2"):
            cleaned["umpire_2"] = str(data["umpire_2"]).strip()
        if data.get("umpire_3"):
            cleaned["umpire_3"] = str(data["umpire_3"]).strip()
        if data.get("match_referee"):
            cleaned["match_referee"] = str(data["match_referee"]).strip()
        if data.get("weather"):
            cleaned["weather"] = str(data["weather"]).strip()
        if data.get("pitch_condition"):
            cleaned["pitch_condition"] = str(data["pitch_condition"]).strip()
        if data.get("espn_id"):
            cleaned["espn_id"] = str(data["espn_id"]).strip()
        if data.get("cricinfo_id"):
            cleaned["cricinfo_id"] = str(data["cricinfo_id"]).strip()
        if data.get("notes"):
            cleaned["notes"] = str(data["notes"]).strip()
        
        # Boolean fields
        cleaned["is_domestic"] = bool(data.get("is_domestic", False))
        cleaned["is_day_night"] = bool(data.get("is_day_night", False))
        
        return cleaned
    
    def _clean_inning_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize inning data."""
        cleaned = {}
        
        # Required fields
        cleaned["match_id"] = int(data.get("match_id", 0))
        cleaned["inning_number"] = int(data.get("inning_number", 1))
        cleaned["batting_team_id"] = int(data.get("batting_team_id", 0))
        cleaned["bowling_team_id"] = int(data.get("bowling_team_id", 0))
        cleaned["status"] = str(data.get("status", "not_started")).lower()
        
        # Score fields
        cleaned["runs_scored"] = int(data.get("runs_scored", 0))
        cleaned["wickets_lost"] = int(data.get("wickets_lost", 0))
        cleaned["overs_bowled"] = int(data.get("overs_bowled", 0))
        cleaned["balls_bowled"] = int(data.get("balls_bowled", 0))
        
        # Extras
        cleaned["byes"] = int(data.get("byes", 0))
        cleaned["leg_byes"] = int(data.get("leg_byes", 0))
        cleaned["wides"] = int(data.get("wides", 0))
        cleaned["no_balls"] = int(data.get("no_balls", 0))
        cleaned["penalty_runs"] = int(data.get("penalty_runs", 0))
        
        # Boolean fields
        cleaned["declared"] = bool(data.get("declared", False))
        cleaned["forfeited"] = bool(data.get("forfeited", False))
        cleaned["follow_on_required"] = bool(data.get("follow_on_required", False))
        cleaned["follow_on_achieved"] = bool(data.get("follow_on_achieved", False))
        
        # Optional fields
        if data.get("espn_id"):
            cleaned["espn_id"] = str(data["espn_id"]).strip()
        
        return cleaned
    
    def _clean_ball_by_ball_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize ball-by-ball data."""
        cleaned = {}
        
        # Required fields
        cleaned["inning_id"] = int(data.get("inning_id", 0))
        cleaned["over_number"] = int(data.get("over_number", 1))
        cleaned["ball_number"] = int(data.get("ball_number", 1))
        cleaned["batsman_id"] = int(data.get("batsman_id", 0))
        cleaned["bowler_id"] = int(data.get("bowler_id", 0))
        
        # Optional player fields
        if data.get("non_striker_id"):
            cleaned["non_striker_id"] = int(data["non_striker_id"])
        if data.get("wicket_player_id"):
            cleaned["wicket_player_id"] = int(data["wicket_player_id"])
        
        # Ball outcome
        cleaned["runs_scored"] = int(data.get("runs_scored", 0))
        cleaned["is_wicket"] = bool(data.get("is_wicket", False))
        if data.get("wicket_type"):
            cleaned["wicket_type"] = str(data["wicket_type"]).lower()
        
        # Extras
        cleaned["is_wide"] = bool(data.get("is_wide", False))
        cleaned["is_no_ball"] = bool(data.get("is_no_ball", False))
        cleaned["is_bye"] = bool(data.get("is_bye", False))
        cleaned["is_leg_bye"] = bool(data.get("is_leg_bye", False))
        
        # Ball details
        if data.get("ball_type"):
            cleaned["ball_type"] = str(data["ball_type"]).lower()
        if data.get("shot_type"):
            cleaned["shot_type"] = str(data["shot_type"]).lower()
        if data.get("fielding_position"):
            cleaned["fielding_position"] = str(data["fielding_position"]).strip()
        
        # Boundary information
        cleaned["is_boundary"] = bool(data.get("is_boundary", False))
        cleaned["is_six"] = bool(data.get("is_six", False))
        cleaned["is_four"] = bool(data.get("is_four", False))
        
        # Optional fields
        if data.get("commentary"):
            cleaned["commentary"] = str(data["commentary"]).strip()
        if data.get("notes"):
            cleaned["notes"] = str(data["notes"]).strip()
        if data.get("espn_id"):
            cleaned["espn_id"] = str(data["espn_id"]).strip()
        
        return cleaned
