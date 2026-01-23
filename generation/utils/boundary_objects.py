from dataclasses import dataclass
from typing import List


@dataclass
class Province:
    """
    Represents a province within an autonomous region.

    Attributes:
        id (int): Unique identifier of the province.
        code_province (str): Official province code.
        name (str): Official name of the province.
        alternative_name (str): Alternative or historical name of the province.
        autonomic_region_code (str): Code of the autonomous region this province belongs to.
        autonomic_region_name (str): Name of the autonomous region.
        total_seats (int): Total number of seats assigned to the province.
        latitude (float): Geographic latitude of the province.
        longitude (float): Geographic longitude of the province.
        population (int): Total population of the province.
        iso_3166_2_code (str): ISO 3166-2 code of the province.
    """

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
    """
    Represents an autonomous region composed of multiple provinces.

    Attributes:
        code (str): Identifier code of the autonomous region.
        name (str): Official name of the autonomous region.
        alternative_name (str): Alternative or localized name.
        senators (int): Number of senators assigned to the region.
        total_seats (int): Total number of seats assigned to the region.
        iso_3166_2_code (str): ISO 3166-2 code of the autonomous region.
        provinces (List[Province]): Provinces belonging to this autonomous region.
    """

    code: str
    name: str
    alternative_name: str
    senators: int
    total_seats: int
    iso_3166_2_code: str
    provinces: List[Province]


@dataclass
class Country:
    """
    Represents a country composed of autonomous regions.

    Attributes:
        regions (List[AutonomousRegion]): Autonomous regions within the country.
    """

    regions: List[AutonomousRegion]


@dataclass
class PoliticalParty:
    """
    Represents a political party.

    Attributes:
        name (str): Name of the political party.
        popularity (float): Popularity score of the party (e.g., percentage of support).
    """

    name: str
    popularity: float


@dataclass
class Parties:
    """
    Container for political parties.

    Attributes:
        parties (List[PoliticalParty]): List of political parties.
    """

    parties: List[PoliticalParty]
