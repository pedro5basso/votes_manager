from dataclasses import dataclass
from typing import List


@dataclass
class Province:
    id: int
    code_province: str
    name: str
    alternative_name: str
    autonomic_region_code: str
    autonomic_region_name: str
    total_seats: int
    latitude: float
    longitude: float
    population: int
    iso_3166_2_code: str


@dataclass
class AutonomousRegion:
    code: str
    name: str
    alternative_name: str
    senators: int
    total_seats: int
    iso_3166_2_code: str
    provinces: List[Province]


@dataclass
class Country:
    regions: List[AutonomousRegion]


@dataclass
class PoliticalParty:
    name: str
    popularity: float


@dataclass
class Parties:
    parties: List[PoliticalParty]