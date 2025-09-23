"""Pydantic schemas for player data validation."""

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, validator


class PlayerBase(BaseModel):
    """Base player schema with common fields."""
    
    name: str = Field(..., min_length=1, max_length=100, description="Player name")
    full_name: Optional[str] = Field(None, max_length=200, description="Player full name")
    team_id: int = Field(..., description="Team ID")
    date_of_birth: Optional[date] = Field(None, description="Date of birth")
    place_of_birth: Optional[str] = Field(None, max_length=100, description="Place of birth")
    nationality: str = Field(..., min_length=1, max_length=50, description="Nationality")
    height_cm: Optional[int] = Field(None, ge=100, le=250, description="Height in centimeters")
    weight_kg: Optional[int] = Field(None, ge=30, le=200, description="Weight in kilograms")
    batting_style: Optional[str] = Field(None, max_length=20, description="Batting style")
    bowling_style: Optional[str] = Field(None, max_length=50, description="Bowling style")
    primary_role: str = Field(..., description="Primary role")
    secondary_role: Optional[str] = Field(None, description="Secondary role")
    is_active: bool = Field(True, description="Whether player is active")
    debut_date: Optional[date] = Field(None, description="Debut date")
    retirement_date: Optional[date] = Field(None, description="Retirement date")
    espn_id: Optional[str] = Field(None, max_length=50, description="ESPN Cricinfo ID")
    cricinfo_id: Optional[str] = Field(None, max_length=50, description="Cricinfo ID")
    
    @validator('primary_role')
    def validate_primary_role(cls, v):
        """Validate primary role."""
        valid_roles = ['Batsman', 'Bowler', 'All-rounder', 'Wicket-keeper']
        if v not in valid_roles:
            raise ValueError(f'Primary role must be one of: {", ".join(valid_roles)}')
        return v
    
    @validator('secondary_role')
    def validate_secondary_role(cls, v):
        """Validate secondary role."""
        if v is not None:
            valid_roles = ['Batsman', 'Bowler', 'All-rounder', 'Wicket-keeper']
            if v not in valid_roles:
                raise ValueError(f'Secondary role must be one of: {", ".join(valid_roles)}')
        return v
    
    @validator('batting_style')
    def validate_batting_style(cls, v):
        """Validate batting style."""
        if v is not None:
            valid_styles = ['Left', 'Right']
            if v not in valid_styles:
                raise ValueError(f'Batting style must be one of: {", ".join(valid_styles)}')
        return v
    
    @validator('retirement_date')
    def validate_retirement_date(cls, v, values):
        """Validate retirement date is after debut date."""
        if v and 'debut_date' in values and values['debut_date']:
            if v < values['debut_date']:
                raise ValueError('Retirement date must be after debut date')
        return v


class PlayerCreate(PlayerBase):
    """Schema for creating a new player."""
    pass


class PlayerUpdate(BaseModel):
    """Schema for updating player information."""
    
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    full_name: Optional[str] = Field(None, max_length=200)
    team_id: Optional[int] = None
    date_of_birth: Optional[date] = None
    place_of_birth: Optional[str] = Field(None, max_length=100)
    nationality: Optional[str] = Field(None, min_length=1, max_length=50)
    height_cm: Optional[int] = Field(None, ge=100, le=250)
    weight_kg: Optional[int] = Field(None, ge=30, le=200)
    batting_style: Optional[str] = Field(None, max_length=20)
    bowling_style: Optional[str] = Field(None, max_length=50)
    primary_role: Optional[str] = None
    secondary_role: Optional[str] = None
    is_active: Optional[bool] = None
    debut_date: Optional[date] = None
    retirement_date: Optional[date] = None
    espn_id: Optional[str] = Field(None, max_length=50)
    cricinfo_id: Optional[str] = Field(None, max_length=50)


class PlayerResponse(PlayerBase):
    """Schema for player response data."""
    
    id: int = Field(..., description="Player ID")
    age: Optional[int] = Field(None, description="Player age")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True
