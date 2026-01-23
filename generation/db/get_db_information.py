import logging
from typing import List

from generation.utils.boundary_objects import AutonomousRegion, Country, Province
from logs.logging_config import setup_logging
from generation.utils.political_parties import political_parties

from generation.db.database_connector import MySQLClient

setup_logging(logging.INFO)

log = logging.getLogger(__name__)


class DataBaseInformationObject:
    """Handles reading and mapping geographical data from the database."""

    def __init__(self, db_client: MySQLClient):
        """
        Init class method.
        Args:
            db_client (MySQLClient): database client object.
        """
        self.db_client: MySQLClient = db_client
        self._country = self.get_country_information()
        self._names_mapped = self.mapping_names_prov2regions()
        self._iso_codes_mapped = self.mapping_iso_code_prov2regions()

    @property
    def country(self) -> Country:
        """Returns the country object with all geographical information."""
        return self._country

    @property
    def mapped_names(self) -> dict[str, str]:
        """Maps province names to their corresponding autonomous region names."""
        return self._names_mapped

    @property
    def mapped_iso_codes(self) -> dict[str, str]:
        """Maps province names to their corresponding ISO 3166-2 region codes."""
        return self._iso_codes_mapped

    def get_country_information(self) -> Country:
        """
        Loads geographical information from the database and builds a Country object.

        Returns:
            Country: Country with all regions and provinces loaded.
        """
        log.info("[DBInfoObject]: Loading provinces from MySQL...")

        # queries
        query_provinces = """SELECT * FROM elecciones_es.provincias;"""
        query_autonomid_reg = """SELECT * FROM elecciones_es.comunidades_autonomas;"""

        self.db_client.connect()
        rows_provinces = self.db_client.fetch_all(query_provinces)
        rows_autonomic = self.db_client.fetch_all(query_autonomid_reg)
        self.db_client.disconnect()

        # mapping database information with boundary objects
        autonomic_regions = list()
        for row_autonomic in rows_autonomic:
            provinces = list()
            code_autonomic = row_autonomic["codigo_comunidad"]
            name_autonomic = row_autonomic["nombre"]
            alt_name_autonomic = row_autonomic["nombre_alternativo"]
            senators = row_autonomic["numero_senadores"]
            seats_autonomic = 0
            iso_code_region = row_autonomic["iso_3166_2_ccaa"]
            for row_prov in rows_provinces:
                if row_autonomic["codigo_comunidad"] == row_prov["codigo_comunidad"]:
                    seats_autonomic += row_prov["total_diputados"]
                    province = Province(
                        id=row_prov["id"],
                        code_province=row_prov["codigo_provincia"],
                        name=row_prov["nombre"],
                        alternative_name=row_prov["nombre_alternativo"],
                        autonomic_region_code=row_prov["codigo_comunidad"],
                        autonomic_region_name=name_autonomic,
                        total_seats=row_prov["total_diputados"],
                        latitude=row_prov["latitud"],
                        longitude=row_prov["longitud"],
                        population=row_prov["habitantes"],
                        iso_3166_2_code=row_prov["iso_3166_2_prov"],
                    )
                    provinces.append(province)
                    log.info(f"[DBInfoObject]: Loaded province: {province}")
            autonomic_region = AutonomousRegion(
                code=code_autonomic,
                name=name_autonomic,
                alternative_name=alt_name_autonomic,
                senators=senators,
                total_seats=seats_autonomic,
                provinces=provinces,
                iso_3166_2_code=iso_code_region,
            )
            autonomic_regions.append(autonomic_region)
            log.info(f"[DBInfoObject]: Loaded autonomic_region: {autonomic_region}")

        country = Country(regions=autonomic_regions)

        log.info(f"[DBInfoObject]: Country successfully loaded: {len(country.regions)}")

        return country

    def mapping_names_prov2regions(self) -> dict:
        """
        Maps province names to autonomous region names.

        Returns:
            dict[str, str]: Province → Region mapping.
        """
        return {
            province.name: region.name
            for region in self._country.regions
            for province in region.provinces
        }

    def mapping_iso_code_prov2regions(self) -> dict:
        """
        Maps province names to ISO 3166-2 region codes.

        Returns:
            dict[str, str]: Province → ISO code mapping.
        """
        return {
            province.name: region.iso_3166_2_code
            for region in self._country.regions
            for province in region.provinces
        }

    def get_political_parties(self) -> List[dict]:
        """
        Returns all political parties.

        Returns:
            list[dict]: Political parties information.
        """
        return political_parties

    def get_seats_by_province(self) -> list[tuple[str, int]]:
        """
        Gets the seats information from the provinces.
            Returns:
                list of tuples: list with the seats information from each province
        """
        return [
            (province.name, province.total_seats)
            for region in self._country.regions
            for province in region.provinces
        ]
