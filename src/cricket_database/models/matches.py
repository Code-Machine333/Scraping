"""Match model for cricket database."""

from datetime import datetime, date
from enum import Enum

from sqlalchemy import Column, String, Integer, DateTime, Date, Boolean, ForeignKey, Text, Index, Enum as SQLEnum
from sqlalchemy.orm import relationship

from .base import Base


class MatchType(str, Enum):
    """Enumeration of cricket match types."""
    TEST = "test"
    ODI = "odi"
    T20 = "t20"
    T20I = "t20i"
    FIRST_CLASS = "first_class"
    LIST_A = "list_a"


class MatchStatus(str, Enum):
    """Enumeration of cricket match statuses."""
    SCHEDULED = "scheduled"
    LIVE = "live"
    COMPLETED = "completed"
    ABANDONED = "abandoned"
    CANCELLED = "cancelled"
    POSTPONED = "postponed"


class Match(Base):
    """Match model representing cricket matches."""
    
    __tablename__ = "matches"
    
    # Core match information
    match_type = Column(SQLEnum(MatchType), nullable=False, index=True)
    status = Column(SQLEnum(MatchStatus), nullable=False, index=True)
    
    # Teams
    home_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    away_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    
    # Match details
    match_date = Column(Date, nullable=False, index=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    
    # Venue information
    venue_name = Column(String(200), nullable=True, index=True)
    venue_city = Column(String(100), nullable=True, index=True)
    venue_country = Column(String(50), nullable=True, index=True)
    venue_capacity = Column(Integer, nullable=True)
    
    # Match context
    series_name = Column(String(200), nullable=True, index=True)
    series_type = Column(String(50), nullable=True, index=True)
    match_number = Column(Integer, nullable=True)  # Match number in series
    total_matches_in_series = Column(Integer, nullable=True)
    
    # Match result
    toss_winner_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    toss_decision = Column(String(20), nullable=True)  # bat, bowl
    match_winner_id = Column(Integer, ForeignKey("teams.id"), nullable=True)
    win_margin = Column(String(50), nullable=True)  # "5 wickets", "45 runs", etc.
    win_type = Column(String(20), nullable=True)  # wickets, runs, tie, no_result
    
    # Match officials
    umpire_1 = Column(String(100), nullable=True)
    umpire_2 = Column(String(100), nullable=True)
    umpire_3 = Column(String(100), nullable=True)  # TV umpire
    match_referee = Column(String(100), nullable=True)
    
    # Weather and conditions
    weather = Column(String(100), nullable=True)
    pitch_condition = Column(String(100), nullable=True)
    
    # External references
    espn_id = Column(String(50), nullable=True, unique=True, index=True)
    cricinfo_id = Column(String(50), nullable=True, unique=True, index=True)
    
    # Additional metadata
    notes = Column(Text, nullable=True)
    is_domestic = Column(Boolean, default=False, nullable=False, index=True)
    is_day_night = Column(Boolean, default=False, nullable=False, index=True)
    
    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_matches")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_matches")
    toss_winner = relationship("Team", foreign_keys=[toss_winner_id])
    match_winner = relationship("Team", foreign_keys=[match_winner_id])
    innings = relationship("Inning", back_populates="match")
    
    # Indexes for common queries
    __table_args__ = (
        Index("idx_match_date_type", "match_date", "match_type"),
        Index("idx_match_teams_date", "home_team_id", "away_team_id", "match_date"),
        Index("idx_match_series", "series_name", "match_number"),
        Index("idx_match_venue_date", "venue_name", "match_date"),
        Index("idx_match_status_date", "status", "match_date"),
    )
    
    @property
    def is_completed(self) -> bool:
        """Check if match is completed."""
        return self.status == MatchStatus.COMPLETED
    
    @property
    def is_live(self) -> bool:
        """Check if match is currently live."""
        return self.status == MatchStatus.LIVE
    
    def __repr__(self) -> str:
        return f"<Match({self.match_type.value}, {self.home_team.name if self.home_team else 'TBD'} vs {self.away_team.name if self.away_team else 'TBD'}, {self.match_date})>"
