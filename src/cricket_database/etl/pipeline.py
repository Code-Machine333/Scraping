"""Main ETL pipeline orchestrator."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .transformers import DataTransformer, DataValidator
from .loaders import DatabaseLoader
from .quality_checks import DataQualityChecker
from ..scrapers import ESPNScraper, CricketAPIScraper
from ..config import settings

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL pipeline for cricket data processing."""
    
    def __init__(
        self,
        enable_validation: bool = True,
        enable_quality_checks: bool = True,
        batch_size: int = 1000,
        dry_run: bool = False
    ):
        self.enable_validation = enable_validation
        self.enable_quality_checks = enable_quality_checks
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        # Initialize components
        self.validator = DataValidator(enable_validation)
        self.transformer = DataTransformer(self.validator)
        self.loader = DatabaseLoader(batch_size)
        self.quality_checker = DataQualityChecker(enable_quality_checks)
        
        # Initialize scrapers
        self.espn_scraper = ESPNScraper(dry_run=dry_run)
        self.cricket_api_scraper = CricketAPIScraper(dry_run=dry_run)
    
    async def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete ETL pipeline."""
        logger.info("Starting full ETL pipeline")
        start_time = datetime.now()
        
        try:
            # Step 1: Extract data
            logger.info("Step 1: Extracting data from sources")
            raw_data = await self._extract_data()
            
            # Step 2: Transform data
            logger.info("Step 2: Transforming data")
            transformed_data = await self._transform_data(raw_data)
            
            # Step 3: Load data
            logger.info("Step 3: Loading data to database")
            load_results = await self._load_data(transformed_data)
            
            # Step 4: Quality checks
            logger.info("Step 4: Running data quality checks")
            quality_results = await self._run_quality_checks()
            
            # Calculate pipeline metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            pipeline_results = {
                "status": "success",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "extraction": {
                    "teams": len(raw_data.get("teams", [])),
                    "players": len(raw_data.get("players", [])),
                    "matches": len(raw_data.get("matches", [])),
                    "innings": len(raw_data.get("innings", [])),
                    "ball_by_ball": len(raw_data.get("ball_by_ball", [])),
                    "player_stats": len(raw_data.get("player_stats", []))
                },
                "transformation": {
                    "teams": len(transformed_data.get("teams", [])),
                    "players": len(transformed_data.get("players", [])),
                    "matches": len(transformed_data.get("matches", [])),
                    "innings": len(transformed_data.get("innings", [])),
                    "ball_by_ball": len(transformed_data.get("ball_by_ball", [])),
                    "player_stats": len(transformed_data.get("player_stats", []))
                },
                "loading": load_results,
                "quality_checks": quality_results,
                "dry_run": self.dry_run
            }
            
            logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
            return pipeline_results
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
                "dry_run": self.dry_run
            }
    
    async def run_incremental_update(self, days_back: int = 7) -> Dict[str, Any]:
        """Run incremental ETL pipeline for recent data."""
        logger.info(f"Starting incremental ETL pipeline for last {days_back} days")
        start_time = datetime.now()
        
        try:
            # Calculate date range
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            
            # Step 1: Extract recent data
            logger.info("Step 1: Extracting recent data")
            raw_data = await self._extract_recent_data(start_date, end_date)
            
            # Step 2: Transform data
            logger.info("Step 2: Transforming recent data")
            transformed_data = await self._transform_data(raw_data)
            
            # Step 3: Load data (upsert mode)
            logger.info("Step 3: Loading recent data to database")
            load_results = await self._load_data(transformed_data)
            
            # Step 4: Quality checks
            logger.info("Step 4: Running data quality checks")
            quality_results = await self._run_quality_checks()
            
            # Calculate pipeline metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            pipeline_results = {
                "status": "success",
                "type": "incremental",
                "date_range": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "extraction": {
                    "teams": len(raw_data.get("teams", [])),
                    "players": len(raw_data.get("players", [])),
                    "matches": len(raw_data.get("matches", [])),
                    "innings": len(raw_data.get("innings", [])),
                    "ball_by_ball": len(raw_data.get("ball_by_ball", [])),
                    "player_stats": len(raw_data.get("player_stats", []))
                },
                "transformation": {
                    "teams": len(transformed_data.get("teams", [])),
                    "players": len(transformed_data.get("players", [])),
                    "matches": len(transformed_data.get("matches", [])),
                    "innings": len(transformed_data.get("innings", [])),
                    "ball_by_ball": len(transformed_data.get("ball_by_ball", [])),
                    "player_stats": len(transformed_data.get("player_stats", []))
                },
                "loading": load_results,
                "quality_checks": quality_results,
                "dry_run": self.dry_run
            }
            
            logger.info(f"Incremental ETL pipeline completed successfully in {duration:.2f} seconds")
            return pipeline_results
            
        except Exception as e:
            logger.error(f"Incremental ETL pipeline failed: {e}")
            return {
                "status": "failed",
                "type": "incremental",
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
                "dry_run": self.dry_run
            }
    
    async def _extract_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract data from all sources."""
        all_data = {
            "teams": [],
            "players": [],
            "matches": [],
            "innings": [],
            "ball_by_ball": [],
            "player_stats": []
        }
        
        # Extract from ESPN Cricinfo
        async with self.espn_scraper:
            try:
                espn_data = await self.espn_scraper.scrape_all()
                all_data["teams"].extend(espn_data.get("teams", []))
                all_data["players"].extend(espn_data.get("players", []))
                all_data["matches"].extend(espn_data.get("matches", []))
            except Exception as e:
                logger.warning(f"ESPN scraper failed: {e}")
        
        # Extract from Cricket API (if available)
        try:
            cricket_api_data = await self.cricket_api_scraper.scrape_all()
            all_data["teams"].extend(cricket_api_data.get("teams", []))
            all_data["players"].extend(cricket_api_data.get("players", []))
            all_data["matches"].extend(cricket_api_data.get("matches", []))
        except Exception as e:
            logger.warning(f"Cricket API scraper failed: {e}")
        
        logger.info(f"Extracted data: {sum(len(v) for v in all_data.values())} total records")
        return all_data
    
    async def _extract_recent_data(self, start_date: datetime.date, end_date: datetime.date) -> Dict[str, List[Dict[str, Any]]]:
        """Extract recent data from sources."""
        all_data = {
            "teams": [],
            "players": [],
            "matches": [],
            "innings": [],
            "ball_by_ball": [],
            "player_stats": []
        }
        
        # Extract recent matches from ESPN Cricinfo
        async with self.espn_scraper:
            try:
                recent_matches = await self.espn_scraper.scrape_matches(
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat()
                )
                all_data["matches"].extend(recent_matches)
                
                # Extract detailed match data including ball-by-ball
                for match in recent_matches:
                    if match.get("espn_id"):
                        match_details = await self.espn_scraper.scrape_match_details(match["espn_id"])
                        all_data["ball_by_ball"].extend(match_details.get("ball_by_ball", []))
                        
            except Exception as e:
                logger.warning(f"ESPN scraper failed for recent data: {e}")
        
        # Extract recent matches from Cricket API
        try:
            recent_matches = await self.cricket_api_scraper.scrape_matches(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat()
            )
            all_data["matches"].extend(recent_matches)
            
            # Extract detailed match data
            for match in recent_matches:
                if match.get("cricket_api_id"):
                    match_details = await self.cricket_api_scraper.scrape_match_details(match["cricket_api_id"])
                    all_data["ball_by_ball"].extend(match_details.get("ball_by_ball", []))
                    
        except Exception as e:
            logger.warning(f"Cricket API scraper failed for recent data: {e}")
        
        logger.info(f"Extracted recent data: {sum(len(v) for v in all_data.values())} total records")
        return all_data
    
    async def _transform_data(self, raw_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """Transform raw data using transformers."""
        transformed_data = {}
        
        # Transform teams
        if raw_data.get("teams"):
            transformed_data["teams"] = self.transformer.transform_teams(raw_data["teams"])
        
        # Transform players
        if raw_data.get("players"):
            transformed_data["players"] = self.transformer.transform_players(raw_data["players"])
        
        # Transform matches
        if raw_data.get("matches"):
            transformed_data["matches"] = self.transformer.transform_matches(raw_data["matches"])
        
        # Transform innings
        if raw_data.get("innings"):
            transformed_data["innings"] = self.transformer.transform_innings(raw_data["innings"])
        
        # Transform ball-by-ball
        if raw_data.get("ball_by_ball"):
            transformed_data["ball_by_ball"] = self.transformer.transform_ball_by_ball(raw_data["ball_by_ball"])
        
        # Transform player stats
        if raw_data.get("player_stats"):
            transformed_data["player_stats"] = self.transformer.transform_player_stats(raw_data["player_stats"])
        
        logger.info(f"Transformed data: {sum(len(v) for v in transformed_data.values())} total records")
        return transformed_data
    
    async def _load_data(self, transformed_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, int]]:
        """Load transformed data to database."""
        if self.dry_run:
            logger.info("Dry run mode - skipping database loading")
            return {"dry_run": True}
        
        return await self.loader.load_all_data(transformed_data)
    
    async def _run_quality_checks(self) -> Dict[str, Any]:
        """Run data quality checks."""
        if not self.enable_quality_checks:
            return {"status": "disabled"}
        
        return await self.quality_checker.check_data_quality()
    
    async def validate_data_sources(self) -> Dict[str, Any]:
        """Validate that data sources are accessible."""
        logger.info("Validating data sources")
        
        validation_results = {
            "espn_cricinfo": {"status": "unknown", "error": None},
            "cricket_api": {"status": "unknown", "error": None}
        }
        
        # Test ESPN Cricinfo
        try:
            async with self.espn_scraper:
                # Try to scrape a small amount of data
                teams = await self.espn_scraper.scrape_teams()
                validation_results["espn_cricinfo"] = {
                    "status": "success",
                    "teams_found": len(teams)
                }
        except Exception as e:
            validation_results["espn_cricinfo"] = {
                "status": "failed",
                "error": str(e)
            }
        
        # Test Cricket API
        try:
            # Try to scrape a small amount of data
            teams = await self.cricket_api_scraper.scrape_teams()
            validation_results["cricket_api"] = {
                "status": "success",
                "teams_found": len(teams)
            }
        except Exception as e:
            validation_results["cricket_api"] = {
                "status": "failed",
                "error": str(e)
            }
        
        logger.info(f"Data source validation completed: {validation_results}")
        return validation_results
    
    async def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and metrics."""
        return {
            "pipeline_config": {
                "enable_validation": self.enable_validation,
                "enable_quality_checks": self.enable_quality_checks,
                "batch_size": self.batch_size,
                "dry_run": self.dry_run
            },
            "scraper_config": {
                "rate_limit": settings.scraper.rate_limit,
                "retry_attempts": settings.scraper.retry_attempts,
                "timeout": settings.scraper.timeout,
                "max_requests_per_minute": settings.scraper.max_requests_per_minute,
                "max_requests_per_hour": settings.scraper.max_requests_per_hour
            },
            "data_quality_config": {
                "enable_validation": settings.data_quality.enable_validation,
                "enable_duplicate_check": settings.data_quality.enable_duplicate_check,
                "batch_size": settings.data_quality.batch_size
            },
            "timestamp": datetime.now().isoformat()
        }
