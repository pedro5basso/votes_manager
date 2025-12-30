from dataclasses import dataclass
from typing import List


@dataclass
class Province:
    id: int
    code_province: str
    name: str
    alternative_name: str
    autonomic_region_code: str
    total_seats: int
    latitude: float
    longitude: float
    population: int


@dataclass
class AutonomousRegion:
    code: str
    name: str
    alternative_name: str
