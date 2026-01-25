import json
import os
import random
from collections import defaultdict
from pathlib import Path

# the file can be found at https://hub.huwise.com/explore/assets/georef-spain-municipio/export/
INPUT_COORDINATES_FILE_PATH = r"D:\tmp\votes\georef-spain-municipio.json"

OUTPUT_DIRECTORY = "../coordinates/files"
DIRECTORY = "files"


class GenerateCoordinatesFiles:
    """
    Generates JSON files with geographic coordinates grouped by province.

    This class reads a GeoJSON-like file containing Spanish municipalities,
    extracts coordinates from points and polygon geometries, and writes one
    JSON file per province with a list of coordinates.
    """

    def __init__(self):
        """
        Initialize the generator and load municipality data.

        Loads the input JSON file containing municipality geographic data
        and initializes an internal dictionary to store coordinates
        grouped by province code.
        """
        self.provinces = defaultdict(list)
        with open(INPUT_COORDINATES_FILE_PATH, "r", encoding="utf-8") as f:
            self.municipalities = json.load(f)

    def generate_files(self):
        """Generate coordinate files for each province.

        Iterates over all municipalities, extracts coordinates from:
        - 2D geo points
        - Polygon geometries
        - MultiPolygon geometries

        Coordinates are grouped by province code and written as individual
        JSON files, one per province.
        """
        for municipality in self.municipalities:
            prov_code = municipality["prov_code"]

            # 2d coordinate
            geo_point = municipality.get("geo_point_2d")
            if geo_point:
                coord = f'{geo_point["lon"]},{geo_point["lat"]}'
                self.provinces[prov_code].append(coord)

            geo_shape = municipality.get("geo_shape", {})
            geometry = geo_shape.get("geometry", {})
            coords = geometry.get("coordinates", [])
            geom_type = geometry.get("type")

            if geom_type == "Polygon":
                # coords -> [ [ [lon, lat], ... ] ]
                rings = coords
            elif geom_type == "MultiPolygon":
                # coords -> [ [ [ [lon, lat], ... ] ], ... ]
                rings = []
                for polygon in coords:
                    rings.extend(polygon)
            else:
                rings = []

            for ring in rings:
                for point in ring:
                    lon, lat = point
                    self.provinces[prov_code].append(f"{lat},{lon}")

        # writing files by province
        for prov_code, coords in self.provinces.items():
            output_path = os.path.join(os.getcwd(), DIRECTORY, f"{prov_code}.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump({"coordinates": coords}, f, ensure_ascii=False, indent=2)


class ProvinceCoordinates:
    """Provides random coordinates for a given province.

    This class loads previously generated province coordinate files and
    allows retrieving a random coordinate for a specific province code.
    """

    def __init__(self):
        """Load coordinate files from the output directory.

        Reads all JSON files in the configured directory and builds an
        in-memory mapping of province codes to coordinate lists.
        """
        self.coordinates_by_province = {}
        self.directory_path = Path(OUTPUT_DIRECTORY)
        for file_path in self.directory_path.glob("*.json"):
            prov_code = file_path.stem  # gets "19" from "19.json"

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            coords = data.get("coordinates", [])
            if coords:
                self.coordinates_by_province[prov_code] = coords

    def get_coordinate_from_province(self, prov_code: str) -> str:
        """Return a random coordinate for a given province.

        Args:
            prov_code (str): Province code used as key.

        Returns:
            str: A random coordinate in "lat,lon" format.

        Raises:
            ValueError: If no coordinates are available for the given province.
        """
        try:
            return random.choice(self.coordinates_by_province[prov_code])
        except KeyError:
            raise ValueError(f"No coordinates available for province code {prov_code}")


if __name__ == "__main__":
    # use it just once to create the files
    gcf = GenerateCoordinatesFiles()
    gcf.generate_files()
