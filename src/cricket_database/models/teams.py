"""Team model for cricket database."""

from sqlalchemy import Column, String, Text, Boolean, Index
from sqlalchemy.orm import relationship

from .base import Base


class Team(Base):
    """Team model representing cricket teams."""
    
    __tablename__ = "teams"
    
    # Core team information
    name = Column(String(100), nullable=False, unique=True, index=True)
    short_name = Column(String(10), nullable=False, unique=True, index=True)
    country = Column(String(50), nullable=False, index=True)
    
    # Optional team details
    logo_url = Column(String(500), nullable=True)
    website_url = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    
    # Status flags
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    is_test_playing = Column(Boolean, default=False, nullable=False, index=True)
    is_odi_playing = Column(Boolean, default=True, nullable=False, index=True)
    is_t20_playing = Column(Boolean, default=True, nullable=False, index=True)
    
    # Relationships
    players = relationship("Player", back_populates="team")
    home_matches = relationship("Match", foreign_keys="Match.home_team_id", back_populates="home_team")
    away_matches = relationship("Match", foreign_keys="Match.away_team_id", back_populates="away_team")
    
    # Indexes for common queries
    __table_args__ = (
        Index("idx_team_country_active", "country", "is_active"),
        Index("idx_team_playing_formats", "is_test_playing", "is_odi_playing", "is_t20_playing"),
    )
    
    def __repr__(self) -> str:
        return f"<Team(name='{self.name}', country='{self.country}')>"
