import logging

from typing import List

from generation.utils.boundary_objects import Province, AutonomousRegion, Country, PoliticalParty, Parties
from generation.utils.logging_config import setup_logging
from generation.utils.political_parties import political_parties

setup_logging(logging.INFO)

log = logging.getLogger(__name__)

class DataBaseInformationObject:
    """
    - en un objeto tener toda la info de las bbdd
    - hacer diccionarios para cachear info
    - Provincias, CCAA y partidos politicos
    """
    def __init__(self, db_client):
        """"""
        self.db_client = db_client
        self._country = self.get_country_information()
        self._names_mapped = self.mapping_names_prov2regions()
        self._iso_codes_mapped = self.mapping_iso_code_prov2regions()


    @property
    def country(self):
        return self._country

    @property
    def mapped_names(self):
        return self._names_mapped

    @property
    def mapped_iso_codes(self):
        return self._iso_codes_mapped

    def get_country_information(self) -> Country:
        """"""
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
            code_autonomic = row_autonomic['codigo_comunidad']
            name_autonomic = row_autonomic['nombre']
            alt_name_autonomic = row_autonomic['nombre_alternativo']
            senators = row_autonomic['numero_senadores']
            seats_autonomic = 0
            iso_code_region = row_autonomic['iso_3166_2_ccaa']
            for row_prov in rows_provinces:
                if row_autonomic['codigo_comunidad'] == row_prov['codigo_comunidad']:
                    seats_autonomic += row_prov['total_diputados']
                    province = Province(
                        id=row_prov['id'],
                        code_province=row_prov['codigo_provincia'],
                        name=row_prov['nombre'],
                        alternative_name=row_prov['nombre_alternativo'],
                        autonomic_region_code=row_prov['codigo_comunidad'],
                        autonomic_region_name=name_autonomic,
                        total_seats=row_prov['total_diputados'],
                        latitude=row_prov['latitud'],
                        longitude=row_prov['longitud'],
                        population=row_prov['habitantes'],
                        iso_3166_2_code=row_prov['iso_3166_2_prov']
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
                iso_3166_2_code=iso_code_region
            )
            autonomic_regions.append(autonomic_region)
            log.info(f"[DBInfoObject]: Loaded autonomic_region: {autonomic_region}")

        country = Country(regions=autonomic_regions)

        log.info(f"[DBInfoObject]: Country successfully loaded: {len(country.regions)}")

        return country


    def mapping_names_prov2regions(self) -> dict:
        """"""
        names_mapped = dict()
        for regions in self._country.regions:
            for province in regions.provinces:
                names_mapped[province.name] = regions.name
        return names_mapped


    def mapping_iso_code_prov2regions(self) -> dict:
        """"""
        iso_codes_mapped = dict()
        for region in self._country.regions:
            for province in region.provinces:
                iso_codes_mapped[province.name] = region.iso_3166_2_code
        return iso_codes_mapped



    def get_political_parties(self) -> List[str]:
        """"""
        return political_parties