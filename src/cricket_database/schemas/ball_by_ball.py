"""Pydantic schemas for ball-by-ball data validation."""

from typing import Optional
from pydantic import BaseModel, Field, validator


class BallByBallBase(BaseModel):
    """Base ball-by-ball schema with common fields."""
    
    inning_id: int = Field(..., description="Inning ID")
    over_number: int = Field(..., ge=1, description="Over number")
    ball_number: int = Field(..., ge=1, le=10, description="Ball number (1-6, or 7+ for extras)")
    batsman_id: int = Field(..., description="Batsman ID")
    bowler_id: int = Field(..., description="Bowler ID")
    non_striker_id: Optional[int] = Field(None, description="Non-striker ID")
    runs_scored: int = Field(0, ge=0, le=6, description="Runs scored off the ball")
    is_wicket: bool = Field(False, description="Whether wicket was taken")
    wicket_type: Optional[str] = Field(None, max_length=20, description="Type of wicket")
    wicket_player_id: Optional[int] = Field(None, description="Player who got out")
    is_wide: bool = Field(False, description="Whether ball was wide")
    is_no_ball: bool = Field(False, description="Whether ball was no-ball")
    is_bye: bool = Field(False, description="Whether runs were byes")
    is_leg_bye: bool = Field(False, description="Whether runs were leg byes")
    ball_type: Optional[str] = Field(None, max_length=20, description="Type of ball")
    shot_type: Optional[str] = Field(None, max_length=20, description="Type of shot")
    fielding_position: Optional[str] = Field(None, max_length=50, description="Fielding position")
    is_boundary: bool = Field(False, description="Whether ball was a boundary")
    is_six: bool = Field(False, description="Whether ball was a six")
    is_four: bool = Field(False, description="Whether ball was a four")
    commentary: Optional[str] = Field(None, max_length=2000, description="Ball commentary")
    notes: Optional[str] = Field(None, max_length=1000, description="Additional notes")
    espn_id: Optional[str] = Field(None, max_length=50, description="ESPN Cricinfo ID")
    
    @validator('wicket_type')
    def validate_wicket_type(cls, v):
        """Validate wicket type."""
        if v is not None:
            valid_types = [
                'bowled', 'caught', 'lbw', 'run_out', 'stumped', 'hit_wicket',
                'obstructing_field', 'handled_ball', 'hit_ball_twice', 'timed_out'
            ]
            if v not in valid_types:
                raise ValueError(f'Wicket type must be one of: {", ".join(valid_types)}')
        return v
    
    @validator('ball_type')
    def validate_ball_type(cls, v):
        """Validate ball type."""
        if v is not None:
            valid_types = [
                'normal', 'yorker', 'bouncer', 'full_toss', 'beamer',
                'slower_ball', 'googly', 'doosra', 'reverse_swing'
            ]
            if v not in valid_types:
                raise ValueError(f'Ball type must be one of: {", ".join(valid_types)}')
        return v
    
    @validator('shot_type')
    def validate_shot_type(cls, v):
        """Validate shot type."""
        if v is not None:
            valid_types = [
                'drive', 'cut', 'pull', 'hook', 'sweep', 'reverse_sweep',
                'flick', 'glance', 'defensive', 'leave', 'block'
            ]
            if v not in valid_types:
                raise ValueError(f'Shot type must be one of: {", ".join(valid_types)}')
        return v
    
    @validator('runs_scored')
    def validate_runs_scored(cls, v, values):
        """Validate runs scored based on ball type."""
        if v > 6 and not values.get('is_wide', False) and not values.get('is_no_ball', False):
            raise ValueError('Runs scored cannot exceed 6 for a legal delivery')
        return v
    
    @validator('is_six', 'is_four')
    def validate_boundary_consistency(cls, v, values, field):
        """Validate boundary consistency."""
        runs = values.get('runs_scored', 0)
        if field.name == 'is_six' and v and runs != 6:
            raise ValueError('is_six must be False when runs_scored is not 6')
        if field.name == 'is_four' and v and runs != 4:
            raise ValueError('is_four must be False when runs_scored is not 4')
        return v


class BallByBallCreate(BallByBallBase):
    """Schema for creating a new ball-by-ball record."""
    pass


class BallByBallUpdate(BaseModel):
    """Schema for updating ball-by-ball information."""
    
    inning_id: Optional[int] = None
    over_number: Optional[int] = Field(None, ge=1)
    ball_number: Optional[int] = Field(None, ge=1, le=10)
    batsman_id: Optional[int] = None
    bowler_id: Optional[int] = None
    non_striker_id: Optional[int] = None
    runs_scored: Optional[int] = Field(None, ge=0, le=6)
    is_wicket: Optional[bool] = None
    wicket_type: Optional[str] = Field(None, max_length=20)
    wicket_player_id: Optional[int] = None
    is_wide: Optional[bool] = None
    is_no_ball: Optional[bool] = None
    is_bye: Optional[bool] = None
    is_leg_bye: Optional[bool] = None
    ball_type: Optional[str] = Field(None, max_length=20)
    shot_type: Optional[str] = Field(None, max_length=20)
    fielding_position: Optional[str] = Field(None, max_length=50)
    is_boundary: Optional[bool] = None
    is_six: Optional[bool] = None
    is_four: Optional[bool] = None
    commentary: Optional[str] = Field(None, max_length=2000)
    notes: Optional[str] = Field(None, max_length=1000)
    espn_id: Optional[str] = Field(None, max_length=50)


class BallByBallResponse(BallByBallBase):
    """Schema for ball-by-ball response data."""
    
    id: int = Field(..., description="Ball-by-ball ID")
    is_legal_delivery: bool = Field(..., description="Whether ball was a legal delivery")
    total_runs: int = Field(..., description="Total runs from the ball")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True
