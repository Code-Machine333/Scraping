"""Pydantic schemas for player statistics data validation."""

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class PlayerMatchStatsBase(BaseModel):
    """Base player match stats schema."""
    
    player_id: int = Field(..., description="Player ID")
    match_id: int = Field(..., description="Match ID")
    team_id: int = Field(..., description="Team ID")
    batting_innings: int = Field(0, ge=0, description="Batting innings")
    runs_scored: int = Field(0, ge=0, description="Runs scored")
    balls_faced: int = Field(0, ge=0, description="Balls faced")
    fours: int = Field(0, ge=0, description="Fours hit")
    sixes: int = Field(0, ge=0, description="Sixes hit")
    strike_rate: float = Field(0.0, ge=0.0, description="Strike rate")
    not_out: bool = Field(False, description="Whether not out")
    bowling_innings: int = Field(0, ge=0, description="Bowling innings")
    overs_bowled: int = Field(0, ge=0, description="Overs bowled")
    balls_bowled: int = Field(0, ge=0, description="Balls bowled")
    runs_conceded: int = Field(0, ge=0, description="Runs conceded")
    wickets_taken: int = Field(0, ge=0, description="Wickets taken")
    maidens: int = Field(0, ge=0, description="Maidens bowled")
    economy_rate: float = Field(0.0, ge=0.0, description="Economy rate")
    bowling_average: float = Field(0.0, ge=0.0, description="Bowling average")
    catches: int = Field(0, ge=0, description="Catches taken")
    stumpings: int = Field(0, ge=0, description="Stumpings made")
    run_outs: int = Field(0, ge=0, description="Run outs effected")
    match_date: date = Field(..., description="Match date")
    match_type: str = Field(..., description="Match type")


class PlayerMatchStatsCreate(PlayerMatchStatsBase):
    """Schema for creating player match statistics."""
    pass


class PlayerMatchStatsResponse(PlayerMatchStatsBase):
    """Schema for player match statistics response."""
    
    id: int = Field(..., description="Stats ID")
    batting_average: float = Field(..., description="Batting average")
    overs_decimal: float = Field(..., description="Overs in decimal format")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True


class PlayerCareerStatsBase(BaseModel):
    """Base player career stats schema."""
    
    player_id: int = Field(..., description="Player ID")
    matches_played: int = Field(0, ge=0, description="Matches played")
    innings_batted: int = Field(0, ge=0, description="Innings batted")
    innings_bowled: int = Field(0, ge=0, description="Innings bowled")
    career_runs: int = Field(0, ge=0, description="Career runs")
    career_balls_faced: int = Field(0, ge=0, description="Career balls faced")
    career_fours: int = Field(0, ge=0, description="Career fours")
    career_sixes: int = Field(0, ge=0, description="Career sixes")
    career_not_outs: int = Field(0, ge=0, description="Career not outs")
    career_high_score: int = Field(0, ge=0, description="Career high score")
    career_centuries: int = Field(0, ge=0, description="Career centuries")
    career_fifties: int = Field(0, ge=0, description="Career fifties")
    career_overs_bowled: int = Field(0, ge=0, description="Career overs bowled")
    career_balls_bowled: int = Field(0, ge=0, description="Career balls bowled")
    career_runs_conceded: int = Field(0, ge=0, description="Career runs conceded")
    career_wickets: int = Field(0, ge=0, description="Career wickets")
    career_maidens: int = Field(0, ge=0, description="Career maidens")
    career_best_figures: Optional[str] = Field(None, max_length=20, description="Career best figures")
    career_catches: int = Field(0, ge=0, description="Career catches")
    career_stumpings: int = Field(0, ge=0, description="Career stumpings")
    career_run_outs: int = Field(0, ge=0, description="Career run outs")
    career_batting_average: float = Field(0.0, ge=0.0, description="Career batting average")
    career_strike_rate: float = Field(0.0, ge=0.0, description="Career strike rate")
    career_bowling_average: float = Field(0.0, ge=0.0, description="Career bowling average")
    career_economy_rate: float = Field(0.0, ge=0.0, description="Career economy rate")
    last_updated: date = Field(..., description="Last updated date")


class PlayerCareerStatsCreate(PlayerCareerStatsBase):
    """Schema for creating player career statistics."""
    pass


class PlayerCareerStatsUpdate(BaseModel):
    """Schema for updating player career statistics."""
    
    matches_played: Optional[int] = Field(None, ge=0)
    innings_batted: Optional[int] = Field(None, ge=0)
    innings_bowled: Optional[int] = Field(None, ge=0)
    career_runs: Optional[int] = Field(None, ge=0)
    career_balls_faced: Optional[int] = Field(None, ge=0)
    career_fours: Optional[int] = Field(None, ge=0)
    career_sixes: Optional[int] = Field(None, ge=0)
    career_not_outs: Optional[int] = Field(None, ge=0)
    career_high_score: Optional[int] = Field(None, ge=0)
    career_centuries: Optional[int] = Field(None, ge=0)
    career_fifties: Optional[int] = Field(None, ge=0)
    career_overs_bowled: Optional[int] = Field(None, ge=0)
    career_balls_bowled: Optional[int] = Field(None, ge=0)
    career_runs_conceded: Optional[int] = Field(None, ge=0)
    career_wickets: Optional[int] = Field(None, ge=0)
    career_maidens: Optional[int] = Field(None, ge=0)
    career_best_figures: Optional[str] = Field(None, max_length=20)
    career_catches: Optional[int] = Field(None, ge=0)
    career_stumpings: Optional[int] = Field(None, ge=0)
    career_run_outs: Optional[int] = Field(None, ge=0)
    career_batting_average: Optional[float] = Field(None, ge=0.0)
    career_strike_rate: Optional[float] = Field(None, ge=0.0)
    career_bowling_average: Optional[float] = Field(None, ge=0.0)
    career_economy_rate: Optional[float] = Field(None, ge=0.0)
    last_updated: Optional[date] = None


class PlayerCareerStatsResponse(PlayerCareerStatsBase):
    """Schema for player career statistics response."""
    
    id: int = Field(..., description="Career stats ID")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True
