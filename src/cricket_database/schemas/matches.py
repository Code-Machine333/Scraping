"""Pydantic schemas for match data validation."""

from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, validator

from ..models.matches import MatchType, MatchStatus


class MatchBase(BaseModel):
    """Base match schema with common fields."""
    
    match_type: MatchType = Field(..., description="Type of match")
    status: MatchStatus = Field(..., description="Match status")
    home_team_id: int = Field(..., description="Home team ID")
    away_team_id: int = Field(..., description="Away team ID")
    match_date: date = Field(..., description="Match date")
    start_time: Optional[datetime] = Field(None, description="Match start time")
    end_time: Optional[datetime] = Field(None, description="Match end time")
    venue_name: Optional[str] = Field(None, max_length=200, description="Venue name")
    venue_city: Optional[str] = Field(None, max_length=100, description="Venue city")
    venue_country: Optional[str] = Field(None, max_length=50, description="Venue country")
    venue_capacity: Optional[int] = Field(None, ge=0, description="Venue capacity")
    series_name: Optional[str] = Field(None, max_length=200, description="Series name")
    series_type: Optional[str] = Field(None, max_length=50, description="Series type")
    match_number: Optional[int] = Field(None, ge=1, description="Match number in series")
    total_matches_in_series: Optional[int] = Field(None, ge=1, description="Total matches in series")
    toss_winner_id: Optional[int] = Field(None, description="Toss winner team ID")
    toss_decision: Optional[str] = Field(None, max_length=20, description="Toss decision")
    match_winner_id: Optional[int] = Field(None, description="Match winner team ID")
    win_margin: Optional[str] = Field(None, max_length=50, description="Win margin")
    win_type: Optional[str] = Field(None, max_length=20, description="Win type")
    umpire_1: Optional[str] = Field(None, max_length=100, description="First umpire")
    umpire_2: Optional[str] = Field(None, max_length=100, description="Second umpire")
    umpire_3: Optional[str] = Field(None, max_length=100, description="Third umpire")
    match_referee: Optional[str] = Field(None, max_length=100, description="Match referee")
    weather: Optional[str] = Field(None, max_length=100, description="Weather conditions")
    pitch_condition: Optional[str] = Field(None, max_length=100, description="Pitch condition")
    espn_id: Optional[str] = Field(None, max_length=50, description="ESPN Cricinfo ID")
    cricinfo_id: Optional[str] = Field(None, max_length=50, description="Cricinfo ID")
    notes: Optional[str] = Field(None, max_length=2000, description="Additional notes")
    is_domestic: bool = Field(False, description="Whether match is domestic")
    is_day_night: bool = Field(False, description="Whether match is day-night")
    
    @validator('toss_decision')
    def validate_toss_decision(cls, v):
        """Validate toss decision."""
        if v is not None:
            valid_decisions = ['bat', 'bowl']
            if v.lower() not in valid_decisions:
                raise ValueError(f'Toss decision must be one of: {", ".join(valid_decisions)}')
        return v
    
    @validator('win_type')
    def validate_win_type(cls, v):
        """Validate win type."""
        if v is not None:
            valid_types = ['wickets', 'runs', 'tie', 'no_result']
            if v.lower() not in valid_types:
                raise ValueError(f'Win type must be one of: {", ".join(valid_types)}')
        return v
    
    @validator('away_team_id')
    def validate_different_teams(cls, v, values):
        """Validate that home and away teams are different."""
        if 'home_team_id' in values and v == values['home_team_id']:
            raise ValueError('Home team and away team must be different')
        return v
    
    @validator('end_time')
    def validate_end_time(cls, v, values):
        """Validate that end time is after start time."""
        if v and 'start_time' in values and values['start_time']:
            if v < values['start_time']:
                raise ValueError('End time must be after start time')
        return v


class MatchCreate(MatchBase):
    """Schema for creating a new match."""
    pass


class MatchUpdate(BaseModel):
    """Schema for updating match information."""
    
    match_type: Optional[MatchType] = None
    status: Optional[MatchStatus] = None
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    match_date: Optional[date] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    venue_name: Optional[str] = Field(None, max_length=200)
    venue_city: Optional[str] = Field(None, max_length=100)
    venue_country: Optional[str] = Field(None, max_length=50)
    venue_capacity: Optional[int] = Field(None, ge=0)
    series_name: Optional[str] = Field(None, max_length=200)
    series_type: Optional[str] = Field(None, max_length=50)
    match_number: Optional[int] = Field(None, ge=1)
    total_matches_in_series: Optional[int] = Field(None, ge=1)
    toss_winner_id: Optional[int] = None
    toss_decision: Optional[str] = Field(None, max_length=20)
    match_winner_id: Optional[int] = None
    win_margin: Optional[str] = Field(None, max_length=50)
    win_type: Optional[str] = Field(None, max_length=20)
    umpire_1: Optional[str] = Field(None, max_length=100)
    umpire_2: Optional[str] = Field(None, max_length=100)
    umpire_3: Optional[str] = Field(None, max_length=100)
    match_referee: Optional[str] = Field(None, max_length=100)
    weather: Optional[str] = Field(None, max_length=100)
    pitch_condition: Optional[str] = Field(None, max_length=100)
    espn_id: Optional[str] = Field(None, max_length=50)
    cricinfo_id: Optional[str] = Field(None, max_length=50)
    notes: Optional[str] = Field(None, max_length=2000)
    is_domestic: Optional[bool] = None
    is_day_night: Optional[bool] = None


class MatchResponse(MatchBase):
    """Schema for match response data."""
    
    id: int = Field(..., description="Match ID")
    is_completed: bool = Field(..., description="Whether match is completed")
    is_live: bool = Field(..., description="Whether match is live")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True
