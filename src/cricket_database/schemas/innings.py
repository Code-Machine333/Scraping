"""Pydantic schemas for inning data validation."""

from typing import Optional
from pydantic import BaseModel, Field, validator

from ..models.innings import InningStatus


class InningBase(BaseModel):
    """Base inning schema with common fields."""
    
    match_id: int = Field(..., description="Match ID")
    inning_number: int = Field(..., ge=1, le=4, description="Inning number (1-4)")
    batting_team_id: int = Field(..., description="Batting team ID")
    bowling_team_id: int = Field(..., description="Bowling team ID")
    status: InningStatus = Field(..., description="Inning status")
    declared: bool = Field(False, description="Whether inning was declared")
    forfeited: bool = Field(False, description="Whether inning was forfeited")
    runs_scored: int = Field(0, ge=0, description="Runs scored")
    wickets_lost: int = Field(0, ge=0, le=10, description="Wickets lost")
    overs_bowled: int = Field(0, ge=0, description="Overs bowled")
    balls_bowled: int = Field(0, ge=0, le=5, description="Balls bowled in last over")
    byes: int = Field(0, ge=0, description="Byes")
    leg_byes: int = Field(0, ge=0, description="Leg byes")
    wides: int = Field(0, ge=0, description="Wides")
    no_balls: int = Field(0, ge=0, description="No balls")
    penalty_runs: int = Field(0, ge=0, description="Penalty runs")
    follow_on_required: bool = Field(False, description="Whether follow-on is required")
    follow_on_achieved: bool = Field(False, description="Whether follow-on was achieved")
    espn_id: Optional[str] = Field(None, max_length=50, description="ESPN Cricinfo ID")
    
    @validator('bowling_team_id')
    def validate_different_teams(cls, v, values):
        """Validate that batting and bowling teams are different."""
        if 'batting_team_id' in values and v == values['batting_team_id']:
            raise ValueError('Batting team and bowling team must be different')
        return v
    
    @validator('balls_bowled')
    def validate_balls_bowled(cls, v):
        """Validate balls bowled in last over."""
        if v > 5:
            raise ValueError('Balls bowled in last over cannot exceed 5')
        return v
    
    @validator('wickets_lost')
    def validate_wickets_lost(cls, v):
        """Validate wickets lost."""
        if v > 10:
            raise ValueError('Wickets lost cannot exceed 10')
        return v


class InningCreate(InningBase):
    """Schema for creating a new inning."""
    pass


class InningUpdate(BaseModel):
    """Schema for updating inning information."""
    
    match_id: Optional[int] = None
    inning_number: Optional[int] = Field(None, ge=1, le=4)
    batting_team_id: Optional[int] = None
    bowling_team_id: Optional[int] = None
    status: Optional[InningStatus] = None
    declared: Optional[bool] = None
    forfeited: Optional[bool] = None
    runs_scored: Optional[int] = Field(None, ge=0)
    wickets_lost: Optional[int] = Field(None, ge=0, le=10)
    overs_bowled: Optional[int] = Field(None, ge=0)
    balls_bowled: Optional[int] = Field(None, ge=0, le=5)
    byes: Optional[int] = Field(None, ge=0)
    leg_byes: Optional[int] = Field(None, ge=0)
    wides: Optional[int] = Field(None, ge=0)
    no_balls: Optional[int] = Field(None, ge=0)
    penalty_runs: Optional[int] = Field(None, ge=0)
    follow_on_required: Optional[bool] = None
    follow_on_achieved: Optional[bool] = None
    espn_id: Optional[str] = Field(None, max_length=50)


class InningResponse(InningBase):
    """Schema for inning response data."""
    
    id: int = Field(..., description="Inning ID")
    total_extras: int = Field(..., description="Total extras")
    overs_decimal: float = Field(..., description="Overs in decimal format")
    run_rate: float = Field(..., description="Run rate")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True
