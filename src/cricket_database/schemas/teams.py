"""Pydantic schemas for team data validation."""

from typing import Optional
from pydantic import BaseModel, Field, HttpUrl


class TeamBase(BaseModel):
    """Base team schema with common fields."""
    
    name: str = Field(..., min_length=1, max_length=100, description="Team name")
    short_name: str = Field(..., min_length=1, max_length=10, description="Team short name")
    country: str = Field(..., min_length=1, max_length=50, description="Team country")
    logo_url: Optional[HttpUrl] = Field(None, description="Team logo URL")
    website_url: Optional[HttpUrl] = Field(None, description="Team website URL")
    description: Optional[str] = Field(None, max_length=1000, description="Team description")
    is_active: bool = Field(True, description="Whether team is active")
    is_test_playing: bool = Field(False, description="Whether team plays Test cricket")
    is_odi_playing: bool = Field(True, description="Whether team plays ODI cricket")
    is_t20_playing: bool = Field(True, description="Whether team plays T20 cricket")


class TeamCreate(TeamBase):
    """Schema for creating a new team."""
    pass


class TeamUpdate(BaseModel):
    """Schema for updating team information."""
    
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    short_name: Optional[str] = Field(None, min_length=1, max_length=10)
    country: Optional[str] = Field(None, min_length=1, max_length=50)
    logo_url: Optional[HttpUrl] = None
    website_url: Optional[HttpUrl] = None
    description: Optional[str] = Field(None, max_length=1000)
    is_active: Optional[bool] = None
    is_test_playing: Optional[bool] = None
    is_odi_playing: Optional[bool] = None
    is_t20_playing: Optional[bool] = None


class TeamResponse(TeamBase):
    """Schema for team response data."""
    
    id: int = Field(..., description="Team ID")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Last update timestamp")
    
    class Config:
        from_attributes = True
