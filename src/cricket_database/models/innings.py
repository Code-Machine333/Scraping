"""Inning model for cricket database."""

from enum import Enum

from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, Index, Enum as SQLEnum
from sqlalchemy.orm import relationship

from .base import Base


class InningStatus(str, Enum):
    """Enumeration of inning statuses."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    DECLARED = "declared"
    FORFEITED = "forfeited"


class Inning(Base):
    """Inning model representing cricket innings."""
    
    __tablename__ = "innings"
    
    # Core inning information
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False, index=True)
    inning_number = Column(Integer, nullable=False, index=True)  # 1, 2, 3, 4
    batting_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    bowling_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    
    # Inning details
    status = Column(SQLEnum(InningStatus), nullable=False, index=True)
    declared = Column(Boolean, default=False, nullable=False)
    forfeited = Column(Boolean, default=False, nullable=False)
    
    # Score information
    runs_scored = Column(Integer, default=0, nullable=False)
    wickets_lost = Column(Integer, default=0, nullable=False)
    overs_bowled = Column(Integer, default=0, nullable=False)  # Total overs as integer
    balls_bowled = Column(Integer, default=0, nullable=False)  # Additional balls in last over
    
    # Extras breakdown
    byes = Column(Integer, default=0, nullable=False)
    leg_byes = Column(Integer, default=0, nullable=False)
    wides = Column(Integer, default=0, nullable=False)
    no_balls = Column(Integer, default=0, nullable=False)
    penalty_runs = Column(Integer, default=0, nullable=False)
    
    # Follow-on information (for Test matches)
    follow_on_required = Column(Boolean, default=False, nullable=False)
    follow_on_achieved = Column(Boolean, default=False, nullable=False)
    
    # External references
    espn_id = Column(String(50), nullable=True, unique=True, index=True)
    
    # Relationships
    match = relationship("Match", back_populates="innings")
    batting_team = relationship("Team", foreign_keys=[batting_team_id])
    bowling_team = relationship("Team", foreign_keys=[bowling_team_id])
    ball_by_ball = relationship("BallByBall", back_populates="inning")
    
    # Indexes for common queries
    __table_args__ = (
        Index("idx_inning_match_number", "match_id", "inning_number"),
        Index("idx_inning_teams", "batting_team_id", "bowling_team_id"),
        Index("idx_inning_status", "status"),
    )
    
    @property
    def total_extras(self) -> int:
        """Calculate total extras."""
        return self.byes + self.leg_byes + self.wides + self.no_balls + self.penalty_runs
    
    @property
    def overs_decimal(self) -> float:
        """Get overs in decimal format (e.g., 45.3 for 45 overs 3 balls)."""
        return self.overs_bowled + (self.balls_bowled / 6.0)
    
    @property
    def run_rate(self) -> float:
        """Calculate run rate."""
        if self.overs_decimal == 0:
            return 0.0
        return round(self.runs_scored / self.overs_decimal, 2)
    
    def __repr__(self) -> str:
        return f"<Inning({self.inning_number}, {self.batting_team.name if self.batting_team else 'TBD'} vs {self.bowling_team.name if self.bowling_team else 'TBD'}, {self.runs_scored}/{self.wickets_lost})>"
