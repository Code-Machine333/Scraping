"""Cricket API scraper implementation."""

import asyncio
from datetime import datetime, date
from typing import Any, Dict, List, Optional

from .base import BaseScraper, ScrapingError


class CricketAPIScraper(BaseScraper):
    """Cricket API scraper for structured cricket data."""
    
    def __init__(self, api_key: Optional[str] = None, dry_run: bool = False):
        super().__init__(
            base_url="https://api.cricket.com",
            dry_run=dry_run
        )
        self.api_key = api_key
        self.headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        } if api_key else {}
    
    async def scrape_teams(self) -> List[Dict[str, Any]]:
        """Scrape team data from Cricket API."""
        logger.info("Scraping teams from Cricket API")
        
        try:
            # Get all teams
            response = await self._make_request(
                "/v1/teams",
                headers=self.headers
            )
            
            if isinstance(response, dict) and "data" in response:
                teams = []
                for team_data in response["data"]:
                    processed_team = self._process_team_data(team_data)
                    if processed_team:
                        teams.append(processed_team)
                
                logger.info(f"Scraped {len(teams)} teams from Cricket API")
                return teams
            else:
                logger.warning("Invalid response format from Cricket API")
                return []
                
        except Exception as e:
            logger.error(f"Failed to scrape teams from Cricket API: {e}")
            return []
    
    async def scrape_players(self, team_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Scrape player data from Cricket API."""
        logger.info("Scraping players from Cricket API")
        
        players = []
        
        try:
            if team_id:
                # Get players for specific team
                response = await self._make_request(
                    f"/v1/teams/{team_id}/players",
                    headers=self.headers
                )
                
                if isinstance(response, dict) and "data" in response:
                    for player_data in response["data"]:
                        processed_player = self._process_player_data(player_data, team_id)
                        if processed_player:
                            players.append(processed_player)
            else:
                # Get all players
                response = await self._make_request(
                    "/v1/players",
                    headers=self.headers
                )
                
                if isinstance(response, dict) and "data" in response:
                    for player_data in response["data"]:
                        processed_player = self._process_player_data(player_data)
                        if processed_player:
                            players.append(processed_player)
            
            logger.info(f"Scraped {len(players)} players from Cricket API")
            return players
            
        except Exception as e:
            logger.error(f"Failed to scrape players from Cricket API: {e}")
            return []
    
    async def scrape_matches(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        match_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Scrape match data from Cricket API."""
        logger.info("Scraping matches from Cricket API")
        
        try:
            # Build query parameters
            params = {}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if match_type:
                params["match_type"] = match_type
            
            response = await self._make_request(
                "/v1/matches",
                headers=self.headers,
                params=params
            )
            
            if isinstance(response, dict) and "data" in response:
                matches = []
                for match_data in response["data"]:
                    processed_match = self._process_match_data(match_data)
                    if processed_match:
                        matches.append(processed_match)
                
                logger.info(f"Scraped {len(matches)} matches from Cricket API")
                return matches
            else:
                logger.warning("Invalid response format from Cricket API")
                return []
                
        except Exception as e:
            logger.error(f"Failed to scrape matches from Cricket API: {e}")
            return []
    
    async def scrape_match_details(self, match_id: str) -> Dict[str, Any]:
        """Scrape detailed match data including ball-by-ball from Cricket API."""
        try:
            # Get match details
            match_response = await self._make_request(
                f"/v1/matches/{match_id}",
                headers=self.headers
            )
            
            # Get ball-by-ball data
            ball_by_ball_response = await self._make_request(
                f"/v1/matches/{match_id}/ball-by-ball",
                headers=self.headers
            )
            
            match_details = {}
            
            if isinstance(match_response, dict) and "data" in match_response:
                match_details.update(self._process_match_data(match_response["data"]))
            
            if isinstance(ball_by_ball_response, dict) and "data" in ball_by_ball_response:
                match_details["ball_by_ball"] = self._process_ball_by_ball_data(ball_by_ball_response["data"])
            else:
                match_details["ball_by_ball"] = []
            
            return match_details
            
        except Exception as e:
            logger.error(f"Failed to scrape match details for {match_id}: {e}")
            return {"match_id": match_id, "ball_by_ball": []}
    
    def _process_team_data(self, team_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process raw team data from API."""
        try:
            return {
                "name": team_data.get("name", ""),
                "short_name": team_data.get("short_name", ""),
                "country": team_data.get("country", ""),
                "logo_url": team_data.get("logo_url"),
                "website_url": team_data.get("website_url"),
                "description": team_data.get("description"),
                "is_active": team_data.get("is_active", True),
                "is_test_playing": team_data.get("is_test_playing", False),
                "is_odi_playing": team_data.get("is_odi_playing", True),
                "is_t20_playing": team_data.get("is_t20_playing", True),
                "cricket_api_id": team_data.get("id"),
                "source": "cricket_api"
            }
        except Exception as e:
            logger.warning(f"Failed to process team data: {e}")
            return None
    
    def _process_player_data(self, player_data: Dict[str, Any], team_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Process raw player data from API."""
        try:
            # Parse date of birth
            dob = None
            if player_data.get("date_of_birth"):
                try:
                    dob = datetime.strptime(player_data["date_of_birth"], "%Y-%m-%d").date()
                except ValueError:
                    pass
            
            # Parse debut date
            debut_date = None
            if player_data.get("debut_date"):
                try:
                    debut_date = datetime.strptime(player_data["debut_date"], "%Y-%m-%d").date()
                except ValueError:
                    pass
            
            # Parse retirement date
            retirement_date = None
            if player_data.get("retirement_date"):
                try:
                    retirement_date = datetime.strptime(player_data["retirement_date"], "%Y-%m-%d").date()
                except ValueError:
                    pass
            
            return {
                "name": player_data.get("name", ""),
                "full_name": player_data.get("full_name"),
                "team_id": team_id or player_data.get("team_id"),
                "date_of_birth": dob,
                "place_of_birth": player_data.get("place_of_birth"),
                "nationality": player_data.get("nationality", ""),
                "height_cm": player_data.get("height_cm"),
                "weight_kg": player_data.get("weight_kg"),
                "batting_style": player_data.get("batting_style"),
                "bowling_style": player_data.get("bowling_style"),
                "primary_role": player_data.get("primary_role", "Batsman"),
                "secondary_role": player_data.get("secondary_role"),
                "is_active": player_data.get("is_active", True),
                "debut_date": debut_date,
                "retirement_date": retirement_date,
                "cricket_api_id": player_data.get("id"),
                "source": "cricket_api"
            }
        except Exception as e:
            logger.warning(f"Failed to process player data: {e}")
            return None
    
    def _process_match_data(self, match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process raw match data from API."""
        try:
            # Parse match date
            match_date = None
            if match_data.get("match_date"):
                try:
                    match_date = datetime.strptime(match_data["match_date"], "%Y-%m-%d").date()
                except ValueError:
                    pass
            
            # Parse start time
            start_time = None
            if match_data.get("start_time"):
                try:
                    start_time = datetime.fromisoformat(match_data["start_time"].replace("Z", "+00:00"))
                except ValueError:
                    pass
            
            # Parse end time
            end_time = None
            if match_data.get("end_time"):
                try:
                    end_time = datetime.fromisoformat(match_data["end_time"].replace("Z", "+00:00"))
                except ValueError:
                    pass
            
            return {
                "match_type": match_data.get("match_type", "odi"),
                "status": match_data.get("status", "scheduled"),
                "home_team_id": match_data.get("home_team_id"),
                "away_team_id": match_data.get("away_team_id"),
                "match_date": match_date,
                "start_time": start_time,
                "end_time": end_time,
                "venue_name": match_data.get("venue", {}).get("name"),
                "venue_city": match_data.get("venue", {}).get("city"),
                "venue_country": match_data.get("venue", {}).get("country"),
                "venue_capacity": match_data.get("venue", {}).get("capacity"),
                "series_name": match_data.get("series", {}).get("name"),
                "series_type": match_data.get("series", {}).get("type"),
                "match_number": match_data.get("match_number"),
                "total_matches_in_series": match_data.get("series", {}).get("total_matches"),
                "toss_winner_id": match_data.get("toss", {}).get("winner_id"),
                "toss_decision": match_data.get("toss", {}).get("decision"),
                "match_winner_id": match_data.get("result", {}).get("winner_id"),
                "win_margin": match_data.get("result", {}).get("margin"),
                "win_type": match_data.get("result", {}).get("type"),
                "umpire_1": match_data.get("officials", {}).get("umpire_1"),
                "umpire_2": match_data.get("officials", {}).get("umpire_2"),
                "umpire_3": match_data.get("officials", {}).get("umpire_3"),
                "match_referee": match_data.get("officials", {}).get("referee"),
                "weather": match_data.get("weather"),
                "pitch_condition": match_data.get("pitch_condition"),
                "cricket_api_id": match_data.get("id"),
                "source": "cricket_api"
            }
        except Exception as e:
            logger.warning(f"Failed to process match data: {e}")
            return None
    
    def _process_ball_by_ball_data(self, ball_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process raw ball-by-ball data from API."""
        processed_balls = []
        
        for ball in ball_data:
            try:
                processed_ball = {
                    "inning_id": ball.get("inning_id"),
                    "over_number": ball.get("over_number"),
                    "ball_number": ball.get("ball_number"),
                    "batsman_id": ball.get("batsman_id"),
                    "bowler_id": ball.get("bowler_id"),
                    "non_striker_id": ball.get("non_striker_id"),
                    "runs_scored": ball.get("runs_scored", 0),
                    "is_wicket": ball.get("is_wicket", False),
                    "wicket_type": ball.get("wicket_type"),
                    "wicket_player_id": ball.get("wicket_player_id"),
                    "is_wide": ball.get("is_wide", False),
                    "is_no_ball": ball.get("is_no_ball", False),
                    "is_bye": ball.get("is_bye", False),
                    "is_leg_bye": ball.get("is_leg_bye", False),
                    "ball_type": ball.get("ball_type"),
                    "shot_type": ball.get("shot_type"),
                    "fielding_position": ball.get("fielding_position"),
                    "is_boundary": ball.get("is_boundary", False),
                    "is_six": ball.get("is_six", False),
                    "is_four": ball.get("is_four", False),
                    "commentary": ball.get("commentary"),
                    "notes": ball.get("notes"),
                    "cricket_api_id": ball.get("id"),
                    "source": "cricket_api"
                }
                
                processed_balls.append(processed_ball)
                
            except Exception as e:
                logger.warning(f"Failed to process ball data: {e}")
                continue
        
        return processed_balls
