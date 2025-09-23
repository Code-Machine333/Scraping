from __future__ import annotations

from typing import List, Optional, Literal

from pydantic import BaseModel, Field


class PlayerRef(BaseModel):
    name: str
    source_key: Optional[str] = None


class TeamRef(BaseModel):
    name: str
    source_key: Optional[str] = None


class VenueRef(BaseModel):
    name: str
    city: Optional[str] = None
    country: Optional[str] = None
    source_key: Optional[str] = None


class BattingEntry(BaseModel):
    player: PlayerRef
    position: Optional[int] = None
    runs: Optional[int] = None
    balls: Optional[int] = None
    minutes: Optional[int] = None
    fours: Optional[int] = None
    sixes: Optional[int] = None
    how_out: Optional[str] = None
    bowler: Optional[PlayerRef] = None
    fielder: Optional[PlayerRef] = None


class BowlingEntry(BaseModel):
    player: PlayerRef
    overs: Optional[float] = None
    maidens: Optional[int] = None
    runs: Optional[int] = None
    wickets: Optional[int] = None
    wides: Optional[int] = None
    no_balls: Optional[int] = None
    econ: Optional[float] = None


class FieldingEntry(BaseModel):
    player: PlayerRef
    catches: Optional[int] = None
    stumpings: Optional[int] = None
    runouts: Optional[int] = None


class Delivery(BaseModel):
    over_no: int
    ball_no: int
    striker: PlayerRef
    non_striker: PlayerRef
    bowler: PlayerRef
    runs_off_bat: int = 0
    extras_bye: int = 0
    extras_legbye: int = 0
    extras_wide: int = 0
    extras_noball: int = 0
    extras_penalty: int = 0
    wicket_type: Optional[str] = None
    dismissal_player: Optional[PlayerRef] = None


class InningsModel(BaseModel):
    innings_no: int
    batting_team: TeamRef
    bowling_team: TeamRef
    runs: Optional[int] = None
    wickets: Optional[int] = None
    overs: Optional[float] = None
    declared: bool = False
    follow_on_enforced: bool = False
    batting: List[BattingEntry] = Field(default_factory=list)
    bowling: List[BowlingEntry] = Field(default_factory=list)
    fielding: List[FieldingEntry] = Field(default_factory=list)
    deliveries: List[Delivery] = Field(default_factory=list)


class Officials(BaseModel):
    umpires: List[PlayerRef] = Field(default_factory=list)
    third_umpire: Optional[PlayerRef] = None
    match_referee: Optional[PlayerRef] = None


class TossInfo(BaseModel):
    winner: Optional[TeamRef] = None
    decision: Optional[Literal["bat", "bowl"]] = None


class ResultInfo(BaseModel):
    result_type: Optional[str] = None  # win/tie/draw/no_result
    winner: Optional[TeamRef] = None


class MatchModel(BaseModel):
    source_match_key: Optional[str] = None
    format: Optional[str] = None
    start_date: Optional[str] = None  # ISO date
    end_date: Optional[str] = None
    venue: Optional[VenueRef] = None
    series_name: Optional[str] = None
    series_key: Optional[str] = None
    teams: List[TeamRef] = Field(default_factory=list)
    day_night: bool = False
    follow_on: bool = False
    dl_method: bool = False
    reserve_day: bool = False
    toss: TossInfo = Field(default_factory=TossInfo)
    result: ResultInfo = Field(default_factory=ResultInfo)
    officials: Officials = Field(default_factory=Officials)
    innings: List[InningsModel] = Field(default_factory=list)
    aliases: List[str] = Field(default_factory=list)  # names observed for later review


