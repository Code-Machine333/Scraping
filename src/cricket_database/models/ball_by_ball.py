"""Ball-by-ball model for cricket database."""

from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, Index, Text
from sqlalchemy.orm import relationship

from .base import Base


class BallByBall(Base):
    """Ball-by-ball model representing individual ball data."""
    
    __tablename__ = "ball_by_ball"
    
    # Core ball information
    inning_id = Column(Integer, ForeignKey("innings.id"), nullable=False, index=True)
    over_number = Column(Integer, nullable=False, index=True)
    ball_number = Column(Integer, nullable=False, index=True)  # 1-6, or 7+ for extras
    
    # Players involved
    batsman_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    bowler_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    non_striker_id = Column(Integer, ForeignKey("players.id"), nullable=True, index=True)
    
    # Ball outcome
    runs_scored = Column(Integer, default=0, nullable=False, index=True)
    is_wicket = Column(Boolean, default=False, nullable=False, index=True)
    wicket_type = Column(String(20), nullable=True, index=True)  # bowled, caught, lbw, run_out, etc.
    wicket_player_id = Column(Integer, ForeignKey("players.id"), nullable=True, index=True)
    
    # Extras
    is_wide = Column(Boolean, default=False, nullable=False, index=True)
    is_no_ball = Column(Boolean, default=False, nullable=False, index=True)
    is_bye = Column(Boolean, default=False, nullable=False, index=True)
    is_leg_bye = Column(Boolean, default=False, nullable=False, index=True)
    
    # Ball details
    ball_type = Column(String(20), nullable=True, index=True)  # normal, yorker, bouncer, etc.
    shot_type = Column(String(20), nullable=True, index=True)  # drive, cut, pull, etc.
    fielding_position = Column(String(50), nullable=True, index=True)  # where ball went
    
    # Additional information
    is_boundary = Column(Boolean, default=False, nullable=False, index=True)
    is_six = Column(Boolean, default=False, nullable=False, index=True)
    is_four = Column(Boolean, default=False, nullable=False, index=True)
    
    # Commentary and notes
    commentary = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    

    espn_id = Column(String(50), nullable=True, unique=True, index=True)
    

    inning = relationship("Inning", back_populates="ball_by_ball")
    batsman = relationship("Player", foreign_keys=[batsman_id], back_populates="ball_by_ball_batting")
    bowler = relationship("Player", foreign_keys=[bowler_id], back_populates="ball_by_ball_bowling")
    non_striker = relationship("Player", foreign_keys=[non_striker_id])
    wicket_player = relationship("Player", foreign_keys=[wicket_player_id])
    
    # Indexes for common queries
    __table_args__ = (
        Index("idx_ball_inning_over", "inning_id", "over_number", "ball_number"),
        Index("idx_ball_batsman", "batsman_id", "runs_scored"),
        Index("idx_ball_bowler", "bowler_id", "is_wicket"),
        Index("idx_ball_wickets", "is_wicket", "wicket_type"),
        Index("idx_ball_boundaries", "is_boundary", "is_six", "is_four"),
        Index("idx_ball_extras", "is_wide", "is_no_ball", "is_bye", "is_leg_bye"),
    )
    
    @property
    def is_legal_delivery(self) -> bool:
        """Check if this is a legal delivery (not wide or no-ball)."""
        return not (self.is_wide or self.is_no_ball)
    
    @property
    def total_runs(self) -> int:
        """Calculate total runs from this ball (including extras)."""
        return self.runs_scored + (1 if self.is_wide else 0) + (1 if self.is_no_ball else 0)
    
    def __repr__(self) -> str:
        return f"<BallByBall(Over {self.over_number}.{self.ball_number}, {self.runs_scored} runs, {'W' if self.is_wicket else ''})>"
