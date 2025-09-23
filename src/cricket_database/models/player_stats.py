"""Player statistics models for cricket database."""

from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, Index, Date
from sqlalchemy.orm import relationship

from .base import Base


class PlayerMatchStats(Base):
    """Player match statistics model."""
    
    __tablename__ = "player_match_stats"
    
    # Core references
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    
    # Batting statistics
    batting_innings = Column(Integer, default=0, nullable=False)
    runs_scored = Column(Integer, default=0, nullable=False)
    balls_faced = Column(Integer, default=0, nullable=False)
    fours = Column(Integer, default=0, nullable=False)
    sixes = Column(Integer, default=0, nullable=False)
    strike_rate = Column(Float, default=0.0, nullable=False)
    not_out = Column(Boolean, default=False, nullable=False)
    
    # Bowling statistics
    bowling_innings = Column(Integer, default=0, nullable=False)
    overs_bowled = Column(Integer, default=0, nullable=False)
    balls_bowled = Column(Integer, default=0, nullable=False)
    runs_conceded = Column(Integer, default=0, nullable=False)
    wickets_taken = Column(Integer, default=0, nullable=False)
    maidens = Column(Integer, default=0, nullable=False)
    economy_rate = Column(Float, default=0.0, nullable=False)
    bowling_average = Column(Float, default=0.0, nullable=False)
    
    # Fielding statistics
    catches = Column(Integer, default=0, nullable=False)
    stumpings = Column(Integer, default=0, nullable=False)
    run_outs = Column(Integer, default=0, nullable=False)
    
    # Match context
    match_date = Column(Date, nullable=False, index=True)
    match_type = Column(String(20), nullable=False, index=True)
    
    # Relationships
    player = relationship("Player", back_populates="match_stats")
    match = relationship("Match")
    team = relationship("Team")
    
    # Indexes for common queries
    __table_args__ = (
        Index("idx_player_match", "player_id", "match_id"),
        Index("idx_player_team_date", "player_id", "team_id", "match_date"),
        Index("idx_match_type_date", "match_type", "match_date"),
    )
    
    @property
    def batting_average(self) -> float:
        """Calculate batting average."""
        if self.batting_innings == 0 or (self.batting_innings == 1 and self.not_out):
            return 0.0
        dismissals = self.batting_innings - (1 if self.not_out else 0)
        return round(self.runs_scored / dismissals, 2) if dismissals > 0 else 0.0
    
    @property
    def overs_decimal(self) -> float:
        """Get overs in decimal format."""
        return self.overs_bowled + (self.balls_bowled / 6.0)
    
    def __repr__(self) -> str:
        return f"<PlayerMatchStats(player='{self.player.name if self.player else 'Unknown'}', match_id={self.match_id}, runs={self.runs_scored}, wickets={self.wickets_taken})>"


class PlayerCareerStats(Base):
    """Player career statistics model."""
    
    __tablename__ = "player_career_stats"
    
    # Core reference
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, unique=True, index=True)
    
    # Match counts
    matches_played = Column(Integer, default=0, nullable=False)
    innings_batted = Column(Integer, default=0, nullable=False)
    innings_bowled = Column(Integer, default=0, nullable=False)
    
    # Batting career stats
    career_runs = Column(Integer, default=0, nullable=False)
    career_balls_faced = Column(Integer, default=0, nullable=False)
    career_fours = Column(Integer, default=0, nullable=False)
    career_sixes = Column(Integer, default=0, nullable=False)
    career_not_outs = Column(Integer, default=0, nullable=False)
    career_high_score = Column(Integer, default=0, nullable=False)
    career_centuries = Column(Integer, default=0, nullable=False)
    career_fifties = Column(Integer, default=0, nullable=False)
    
    # Bowling career stats
    career_overs_bowled = Column(Integer, default=0, nullable=False)
    career_balls_bowled = Column(Integer, default=0, nullable=False)
    career_runs_conceded = Column(Integer, default=0, nullable=False)
    career_wickets = Column(Integer, default=0, nullable=False)
    career_maidens = Column(Integer, default=0, nullable=False)
    career_best_figures = Column(String(20), nullable=True)  # "5/20"
    
    # Fielding career stats
    career_catches = Column(Integer, default=0, nullable=False)
    career_stumpings = Column(Integer, default=0, nullable=False)
    career_run_outs = Column(Integer, default=0, nullable=False)
    
    # Calculated averages and rates
    career_batting_average = Column(Float, default=0.0, nullable=False)
    career_strike_rate = Column(Float, default=0.0, nullable=False)
    career_bowling_average = Column(Float, default=0.0, nullable=False)
    career_economy_rate = Column(Float, default=0.0, nullable=False)
    
    # Last updated
    last_updated = Column(Date, nullable=False, index=True)
    
    # Relationships
    player = relationship("Player", back_populates="career_stats")
    
    def __repr__(self) -> str:
        return f"<PlayerCareerStats(player='{self.player.name if self.player else 'Unknown'}', runs={self.career_runs}, wickets={self.career_wickets})>"
