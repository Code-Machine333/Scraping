"""Player model for cricket database."""

from datetime import date
from typing import Optional

from sqlalchemy import Column, String, Integer, Date, Boolean, ForeignKey, Index
from sqlalchemy.orm import relationship

from .base import Base


class Player(Base):
    """Player model representing cricket players."""
    
    __tablename__ = "players"
    
    # Core player information
    name = Column(String(100), nullable=False, index=True)
    full_name = Column(String(200), nullable=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    
    # Personal details
    date_of_birth = Column(Date, nullable=True, index=True)
    place_of_birth = Column(String(100), nullable=True)
    nationality = Column(String(50), nullable=False, index=True)
    
    # Physical attributes
    height_cm = Column(Integer, nullable=True)
    weight_kg = Column(Integer, nullable=True)
    batting_style = Column(String(20), nullable=True, index=True)  # Left, Right
    bowling_style = Column(String(50), nullable=True, index=True)  # Right-arm fast, Left-arm orthodox, etc.
    
    # Role information
    primary_role = Column(String(20), nullable=False, index=True)  # Batsman, Bowler, All-rounder, Wicket-keeper
    secondary_role = Column(String(20), nullable=True, index=True)
    
    # Status and career info
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    debut_date = Column(Date, nullable=True, index=True)
    retirement_date = Column(Date, nullable=True, index=True)
    
    # External references
    espn_id = Column(String(50), nullable=True, unique=True, index=True)
    cricinfo_id = Column(String(50), nullable=True, unique=True, index=True)
    
    # Relationships
    team = relationship("Team", back_populates="players")
    match_stats = relationship("PlayerMatchStats", back_populates="player")
    career_stats = relationship("PlayerCareerStats", back_populates="player")
    ball_by_ball_batting = relationship("BallByBall", foreign_keys="BallByBall.batsman_id", back_populates="batsman")
    ball_by_ball_bowling = relationship("BallByBall", foreign_keys="BallByBall.bowler_id", back_populates="bowler")
    
    # Indexes for common queries
    __table_args__ = (
        Index("idx_player_team_active", "team_id", "is_active"),
        Index("idx_player_role_nationality", "primary_role", "nationality"),
        Index("idx_player_birth_year", "date_of_birth"),
    )
    
    @property
    def age(self) -> Optional[int]:
        """Calculate player's age."""
        if not self.date_of_birth:
            return None
        today = date.today()
        return today.year - self.date_of_birth.year - (
            (today.month, today.day) < (self.date_of_birth.month, self.date_of_birth.day)
        )
    
    def __repr__(self) -> str:
        return f"<Player(name='{self.name}', team='{self.team.name if self.team else 'Unknown'}')>"
