"""ESPN Cricinfo scraper implementation."""

import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from lxml import html, etree

from .base import BaseScraper, ScrapingError


class ESPNScraper(BaseScraper):
    """ESPN Cricinfo scraper for cricket data."""
    
    def __init__(self, dry_run: bool = False):
        super().__init__(
            base_url="https://www.espncricinfo.com",
            dry_run=dry_run
        )
    
    async def scrape_teams(self) -> List[Dict[str, Any]]:
        """Scrape team data from ESPN Cricinfo."""
        logger.info("Scraping teams from ESPN Cricinfo")
        
        teams = []
        
        # Scrape international teams
        international_teams = await self._scrape_international_teams()
        teams.extend(international_teams)
        
        # Scrape domestic teams (major leagues)
        domestic_teams = await self._scrape_domestic_teams()
        teams.extend(domestic_teams)
        
        logger.info(f"Scraped {len(teams)} teams")
        return teams
    
    async def _scrape_international_teams(self) -> List[Dict[str, Any]]:
        """Scrape international cricket teams."""
        url = "/cricket/teams"
        content = await self._make_request(url, use_browser=True)
        
        if isinstance(content, str):
            tree = html.fromstring(content)
            
            teams = []
            team_links = tree.xpath('//a[contains(@href, "/cricket/team/")]')
            
            for link in team_links:
                try:
                    team_name = link.text_content().strip()
                    team_url = link.get('href')
                    
                    if team_name and team_url:
                        team_data = await self._scrape_team_details(team_url)
                        if team_data:
                            teams.append(team_data)
                            
                except Exception as e:
                    logger.warning(f"Failed to scrape team {link.text_content()}: {e}")
                    continue
            
            return teams
        
        return []
    
    async def _scrape_domestic_teams(self) -> List[Dict[str, Any]]:
        """Scrape domestic cricket teams from major leagues."""
        # Major domestic leagues
        leagues = [
            "/cricket/series/ipl-2024-1385561",
            "/cricket/series/big-bash-league-2023-24-1385561",
            "/cricket/series/psl-2024-1385561",
            "/cricket/series/cpl-2024-1385561"
        ]
        
        teams = []
        
        for league_url in leagues:
            try:
                league_teams = await self._scrape_league_teams(league_url)
                teams.extend(league_teams)
            except Exception as e:
                logger.warning(f"Failed to scrape league {league_url}: {e}")
                continue
        
        return teams
    
    async def _scrape_team_details(self, team_url: str) -> Optional[Dict[str, Any]]:
        """Scrape detailed team information."""
        try:
            content = await self._make_request(team_url, use_browser=True)
            
            if isinstance(content, str):
                tree = html.fromstring(content)
                
                # Extract team information
                team_name = self._extract_team_name(tree)
                short_name = self._extract_team_short_name(tree, team_url)
                country = self._extract_team_country(tree)
                
                if not team_name:
                    return None
                
                return {
                    "name": team_name,
                    "short_name": short_name,
                    "country": country,
                    "is_active": True,
                    "is_test_playing": self._is_test_playing_team(team_name),
                    "is_odi_playing": True,
                    "is_t20_playing": True,
                    "espn_id": self._extract_team_id(team_url),
                    "source": "espn_cricinfo"
                }
                
        except Exception as e:
            logger.warning(f"Failed to scrape team details from {team_url}: {e}")
        
        return None
    
    async def _scrape_league_teams(self, league_url: str) -> List[Dict[str, Any]]:
        """Scrape teams from a specific league."""
        content = await self._make_request(league_url, use_browser=True)
        
        if isinstance(content, str):
            tree = html.fromstring(content)
            teams = []
            
            # Look for team links in the league page
            team_links = tree.xpath('//a[contains(@href, "/cricket/team/")]')
            
            for link in team_links:
                try:
                    team_name = link.text_content().strip()
                    team_url = link.get('href')
                    
                    if team_name and team_url:
                        team_data = await self._scrape_team_details(team_url)
                        if team_data:
                            team_data["is_domestic"] = True
                            teams.append(team_data)
                            
                except Exception as e:
                    logger.warning(f"Failed to scrape league team {link.text_content()}: {e}")
                    continue
            
            return teams
        
        return []
    
    async def scrape_players(self, team_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Scrape player data from ESPN Cricinfo."""
        logger.info("Scraping players from ESPN Cricinfo")
        
        players = []
        
        if team_id:
            # Scrape players for specific team
            team_players = await self._scrape_team_players(team_id)
            players.extend(team_players)
        else:
            # Scrape players from all teams
            teams = await self.scrape_teams()
            for team in teams:
                if team.get("espn_id"):
                    team_players = await self._scrape_team_players(team["espn_id"])
                    players.extend(team_players)
        
        logger.info(f"Scraped {len(players)} players")
        return players
    
    async def _scrape_team_players(self, team_id: str) -> List[Dict[str, Any]]:
        """Scrape players for a specific team."""
        url = f"/cricket/team/{team_id}/players"
        content = await self._make_request(url, use_browser=True)
        
        if isinstance(content, str):
            tree = html.fromstring(content)
            players = []
            
            player_links = tree.xpath('//a[contains(@href, "/cricket/player/")]')
            
            for link in player_links:
                try:
                    player_name = link.text_content().strip()
                    player_url = link.get('href')
                    
                    if player_name and player_url:
                        player_data = await self._scrape_player_details(player_url, team_id)
                        if player_data:
                            players.append(player_data)
                            
                except Exception as e:
                    logger.warning(f"Failed to scrape player {link.text_content()}: {e}")
                    continue
            
            return players
        
        return []
    
    async def _scrape_player_details(self, player_url: str, team_id: str) -> Optional[Dict[str, Any]]:
        """Scrape detailed player information."""
        try:
            content = await self._make_request(player_url, use_browser=True)
            
            if isinstance(content, str):
                tree = html.fromstring(content)
                
                # Extract player information
                name = self._extract_player_name(tree)
                full_name = self._extract_player_full_name(tree)
                date_of_birth = self._extract_player_dob(tree)
                place_of_birth = self._extract_player_pob(tree)
                nationality = self._extract_player_nationality(tree)
                batting_style = self._extract_batting_style(tree)
                bowling_style = self._extract_bowling_style(tree)
                primary_role = self._extract_primary_role(tree)
                
                if not name:
                    return None
                
                return {
                    "name": name,
                    "full_name": full_name,
                    "team_id": team_id,
                    "date_of_birth": date_of_birth,
                    "place_of_birth": place_of_birth,
                    "nationality": nationality,
                    "batting_style": batting_style,
                    "bowling_style": bowling_style,
                    "primary_role": primary_role,
                    "is_active": True,
                    "espn_id": self._extract_player_id(player_url),
                    "source": "espn_cricinfo"
                }
                
        except Exception as e:
            logger.warning(f"Failed to scrape player details from {player_url}: {e}")
        
        return None
    
    async def scrape_matches(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        match_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Scrape match data from ESPN Cricinfo."""
        logger.info("Scraping matches from ESPN Cricinfo")
        
        matches = []
        
        # Scrape recent matches
        recent_matches = await self._scrape_recent_matches()
        matches.extend(recent_matches)
        
        # Scrape upcoming matches
        upcoming_matches = await self._scrape_upcoming_matches()
        matches.extend(upcoming_matches)
        
        logger.info(f"Scraped {len(matches)} matches")
        return matches
    
    async def _scrape_recent_matches(self) -> List[Dict[str, Any]]:
        """Scrape recent completed matches."""
        url = "/cricket/scores"
        content = await self._make_request(url, use_browser=True)
        
        if isinstance(content, str):
            tree = html.fromstring(content)
            matches = []
            
            # Look for match scorecards
            match_links = tree.xpath('//a[contains(@href, "/cricket/series/") and contains(@href, "/match/")]')
            
            for link in match_links:
                try:
                    match_url = link.get('href')
                    if match_url:
                        match_data = await self._scrape_match_summary(match_url)
                        if match_data:
                            matches.append(match_data)
                            
                except Exception as e:
                    logger.warning(f"Failed to scrape match {link.text_content()}: {e}")
                    continue
            
            return matches
        
        return []
    
    async def _scrape_upcoming_matches(self) -> List[Dict[str, Any]]:
        """Scrape upcoming matches."""
        url = "/cricket/schedule"
        content = await self._make_request(url, use_browser=True)
        
        if isinstance(content, str):
            tree = html.fromstring(content)
            matches = []
            
            # Look for upcoming match links
            match_links = tree.xpath('//a[contains(@href, "/cricket/series/") and contains(@href, "/match/")]')
            
            for link in match_links:
                try:
                    match_url = link.get('href')
                    if match_url:
                        match_data = await self._scrape_match_summary(match_url)
                        if match_data:
                            matches.append(match_data)
                            
                except Exception as e:
                    logger.warning(f"Failed to scrape upcoming match {link.text_content()}: {e}")
                    continue
            
            return matches
        
        return []
    
    async def _scrape_match_summary(self, match_url: str) -> Optional[Dict[str, Any]]:
        """Scrape match summary information."""
        try:
            content = await self._make_request(match_url, use_browser=True)
            
            if isinstance(content, str):
                tree = html.fromstring(content)
                
                # Extract match information
                match_type = self._extract_match_type(tree)
                teams = self._extract_match_teams(tree)
                match_date = self._extract_match_date(tree)
                venue = self._extract_match_venue(tree)
                series_name = self._extract_series_name(tree)
                
                if not teams or len(teams) < 2:
                    return None
                
                return {
                    "match_type": match_type,
                    "home_team_id": teams[0]["id"],
                    "away_team_id": teams[1]["id"],
                    "match_date": match_date,
                    "venue_name": venue.get("name"),
                    "venue_city": venue.get("city"),
                    "venue_country": venue.get("country"),
                    "series_name": series_name,
                    "status": "completed",  # Default for now
                    "espn_id": self._extract_match_id(match_url),
                    "source": "espn_cricinfo"
                }
                
        except Exception as e:
            logger.warning(f"Failed to scrape match summary from {match_url}: {e}")
        
        return None
    
    async def scrape_match_details(self, match_id: str) -> Dict[str, Any]:
        """Scrape detailed match data including ball-by-ball."""
        url = f"/cricket/series/match/{match_id}/ball-by-ball"
        content = await self._make_request(url, use_browser=True)
        
        if isinstance(content, str):
            tree = html.fromstring(content)
            
            # Extract ball-by-ball data
            ball_by_ball = self._extract_ball_by_ball_data(tree)
            
            return {
                "match_id": match_id,
                "ball_by_ball": ball_by_ball,
                "source": "espn_cricinfo"
            }
        
        return {"match_id": match_id, "ball_by_ball": []}
    
    def _extract_ball_by_ball_data(self, tree: etree._Element) -> List[Dict[str, Any]]:
        """Extract ball-by-ball data from match page."""
        balls = []
        
        # Look for ball-by-ball data in the page
        ball_elements = tree.xpath('//div[contains(@class, "ball")]')
        
        for ball_element in ball_elements:
            try:
                ball_data = {
                    "over_number": self._extract_over_number(ball_element),
                    "ball_number": self._extract_ball_number(ball_element),
                    "runs_scored": self._extract_runs_scored(ball_element),
                    "is_wicket": self._extract_is_wicket(ball_element),
                    "wicket_type": self._extract_wicket_type(ball_element),
                    "is_wide": self._extract_is_wide(ball_element),
                    "is_no_ball": self._extract_is_no_ball(ball_element),
                    "commentary": self._extract_ball_commentary(ball_element)
                }
                
                if ball_data["over_number"] and ball_data["ball_number"]:
                    balls.append(ball_data)
                    
            except Exception as e:
                logger.warning(f"Failed to extract ball data: {e}")
                continue
        
        return balls
    
    # Helper methods for data extraction
    def _extract_team_name(self, tree: etree._Element) -> Optional[str]:
        """Extract team name from page."""
        name_elements = tree.xpath('//h1[contains(@class, "team-name")] | //h1[contains(@class, "name")]')
        return name_elements[0].text_content().strip() if name_elements else None
    
    def _extract_team_short_name(self, tree: etree._Element, url: str) -> str:
        """Extract team short name from URL or page."""
        # Try to extract from URL first
        match = re.search(r'/team/([^/]+)', url)
        if match:
            return match.group(1).upper()
        
        # Fallback to page content
        short_elements = tree.xpath('//span[contains(@class, "short-name")]')
        return short_elements[0].text_content().strip() if short_elements else ""
    
    def _extract_team_country(self, tree: etree._Element) -> str:
        """Extract team country from page."""
        country_elements = tree.xpath('//span[contains(@class, "country")] | //div[contains(@class, "country")]')
        return country_elements[0].text_content().strip() if country_elements else "Unknown"
    
    def _extract_team_id(self, url: str) -> str:
        """Extract team ID from URL."""
        match = re.search(r'/team/([^/]+)', url)
        return match.group(1) if match else ""
    
    def _is_test_playing_team(self, team_name: str) -> bool:
        """Check if team is a Test playing nation."""
        test_teams = [
            "Australia", "England", "India", "Pakistan", "South Africa",
            "West Indies", "New Zealand", "Sri Lanka", "Bangladesh",
            "Zimbabwe", "Afghanistan", "Ireland"
        ]
        return team_name in test_teams
    
    def _extract_player_name(self, tree: etree._Element) -> Optional[str]:
        """Extract player name from page."""
        name_elements = tree.xpath('//h1[contains(@class, "player-name")] | //h1[contains(@class, "name")]')
        return name_elements[0].text_content().strip() if name_elements else None
    
    def _extract_player_full_name(self, tree: etree._Element) -> Optional[str]:
        """Extract player full name from page."""
        full_name_elements = tree.xpath('//div[contains(@class, "full-name")]')
        return full_name_elements[0].text_content().strip() if full_name_elements else None
    
    def _extract_player_dob(self, tree: etree._Element) -> Optional[date]:
        """Extract player date of birth from page."""
        dob_elements = tree.xpath('//span[contains(text(), "Born")] | //div[contains(text(), "Born")]')
        if dob_elements:
            dob_text = dob_elements[0].text_content()
            # Extract date from text
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', dob_text)
            if date_match:
                try:
                    return datetime.strptime(date_match.group(1), "%d %B %Y").date()
                except ValueError:
                    pass
        return None
    
    def _extract_player_pob(self, tree: etree._Element) -> Optional[str]:
        """Extract player place of birth from page."""
        pob_elements = tree.xpath('//span[contains(text(), "Born")] | //div[contains(text(), "Born")]')
        if pob_elements:
            pob_text = pob_elements[0].text_content()
            # Extract place from text
            place_match = re.search(r'Born\s+(.+?)(?:\s+\d|$)', pob_text)
            if place_match:
                return place_match.group(1).strip()
        return None
    
    def _extract_player_nationality(self, tree: etree._Element) -> str:
        """Extract player nationality from page."""
        nationality_elements = tree.xpath('//span[contains(@class, "nationality")] | //div[contains(@class, "nationality")]')
        return nationality_elements[0].text_content().strip() if nationality_elements else "Unknown"
    
    def _extract_batting_style(self, tree: etree._Element) -> Optional[str]:
        """Extract batting style from page."""
        style_elements = tree.xpath('//span[contains(text(), "Batting")] | //div[contains(text(), "Batting")]')
        if style_elements:
            style_text = style_elements[0].text_content()
            if "Left" in style_text:
                return "Left"
            elif "Right" in style_text:
                return "Right"
        return None
    
    def _extract_bowling_style(self, tree: etree._Element) -> Optional[str]:
        """Extract bowling style from page."""
        style_elements = tree.xpath('//span[contains(text(), "Bowling")] | //div[contains(text(), "Bowling")]')
        if style_elements:
            return style_elements[0].text_content().strip()
        return None
    
    def _extract_primary_role(self, tree: etree._Element) -> str:
        """Extract primary role from page."""
        role_elements = tree.xpath('//span[contains(@class, "role")] | //div[contains(@class, "role")]')
        if role_elements:
            role_text = role_elements[0].text_content().lower()
            if "batsman" in role_text:
                return "Batsman"
            elif "bowler" in role_text:
                return "Bowler"
            elif "all-rounder" in role_text:
                return "All-rounder"
            elif "wicket-keeper" in role_text:
                return "Wicket-keeper"
        return "Batsman"  # Default
    
    def _extract_player_id(self, url: str) -> str:
        """Extract player ID from URL."""
        match = re.search(r'/player/([^/]+)', url)
        return match.group(1) if match else ""
    
    def _extract_match_type(self, tree: etree._Element) -> str:
        """Extract match type from page."""
        type_elements = tree.xpath('//span[contains(@class, "match-type")] | //div[contains(@class, "match-type")]')
        if type_elements:
            type_text = type_elements[0].text_content().lower()
            if "test" in type_text:
                return "test"
            elif "odi" in type_text:
                return "odi"
            elif "t20" in type_text:
                return "t20"
        return "odi"  # Default
    
    def _extract_match_teams(self, tree: etree._Element) -> List[Dict[str, str]]:
        """Extract match teams from page."""
        teams = []
        team_elements = tree.xpath('//div[contains(@class, "team")]')
        
        for team_element in team_elements:
            team_name = team_element.text_content().strip()
            if team_name:
                teams.append({"name": team_name, "id": team_name.lower().replace(" ", "_")})
        
        return teams
    
    def _extract_match_date(self, tree: etree._Element) -> Optional[date]:
        """Extract match date from page."""
        date_elements = tree.xpath('//span[contains(@class, "date")] | //div[contains(@class, "date")]')
        if date_elements:
            date_text = date_elements[0].text_content()
            # Try to parse date
            try:
                return datetime.strptime(date_text, "%d %B %Y").date()
            except ValueError:
                pass
        return None
    
    def _extract_match_venue(self, tree: etree._Element) -> Dict[str, str]:
        """Extract match venue from page."""
        venue_elements = tree.xpath('//span[contains(@class, "venue")] | //div[contains(@class, "venue")]')
        if venue_elements:
            venue_text = venue_elements[0].text_content()
            # Parse venue information
            parts = venue_text.split(",")
            return {
                "name": parts[0].strip() if parts else "",
                "city": parts[1].strip() if len(parts) > 1 else "",
                "country": parts[2].strip() if len(parts) > 2 else ""
            }
        return {"name": "", "city": "", "country": ""}
    
    def _extract_series_name(self, tree: etree._Element) -> Optional[str]:
        """Extract series name from page."""
        series_elements = tree.xpath('//span[contains(@class, "series")] | //div[contains(@class, "series")]')
        return series_elements[0].text_content().strip() if series_elements else None
    
    def _extract_match_id(self, url: str) -> str:
        """Extract match ID from URL."""
        match = re.search(r'/match/([^/]+)', url)
        return match.group(1) if match else ""
    
    # Ball-by-ball extraction methods
    def _extract_over_number(self, element: etree._Element) -> Optional[int]:
        """Extract over number from ball element."""
        over_elements = element.xpath('.//span[contains(@class, "over")]')
        if over_elements:
            try:
                return int(over_elements[0].text_content())
            except ValueError:
                pass
        return None
    
    def _extract_ball_number(self, element: etree._Element) -> Optional[int]:
        """Extract ball number from ball element."""
        ball_elements = element.xpath('.//span[contains(@class, "ball")]')
        if ball_elements:
            try:
                return int(ball_elements[0].text_content())
            except ValueError:
                pass
        return None
    
    def _extract_runs_scored(self, element: etree._Element) -> int:
        """Extract runs scored from ball element."""
        runs_elements = element.xpath('.//span[contains(@class, "runs")]')
        if runs_elements:
            try:
                return int(runs_elements[0].text_content())
            except ValueError:
                pass
        return 0
    
    def _extract_is_wicket(self, element: etree._Element) -> bool:
        """Extract wicket information from ball element."""
        wicket_elements = element.xpath('.//span[contains(@class, "wicket")]')
        return len(wicket_elements) > 0
    
    def _extract_wicket_type(self, element: etree._Element) -> Optional[str]:
        """Extract wicket type from ball element."""
        wicket_elements = element.xpath('.//span[contains(@class, "wicket")]')
        if wicket_elements:
            return wicket_elements[0].text_content().strip()
        return None
    
    def _extract_is_wide(self, element: etree._Element) -> bool:
        """Extract wide information from ball element."""
        wide_elements = element.xpath('.//span[contains(@class, "wide")]')
        return len(wide_elements) > 0
    
    def _extract_is_no_ball(self, element: etree._Element) -> bool:
        """Extract no-ball information from ball element."""
        noball_elements = element.xpath('.//span[contains(@class, "noball")]')
        return len(noball_elements) > 0
    
    def _extract_ball_commentary(self, element: etree._Element) -> Optional[str]:
        """Extract ball commentary from ball element."""
        commentary_elements = element.xpath('.//span[contains(@class, "commentary")]')
        return commentary_elements[0].text_content().strip() if commentary_elements else None
