class DataLakeConfig:
    """
    Configuration paths for the data lake storage layers and checkpoints.

    This class defines base paths for the data lake as well as specific
    directories for bronze, silver, and gold layers, including their
    corresponding Spark checkpoint locations.
    """

    PATH_BASE = "/mnt/datalake"
    PATH_CHECKPOINT = "/mnt/checkpoints"
    PATH_STATES = "/mnt/spark-state"

    # Bronze layer
    PATH_BRONZE = f"{PATH_BASE}/bronze/votes"
    CHECKPOINT_BRONZE = f"{PATH_CHECKPOINT}/bronze/votes"

    # Silver layer
    PATH_SILVER = f"{PATH_BASE}/silver"
    CHECKPOINT_SILVER = f"{PATH_CHECKPOINT}/silver"

    PATH_SILVER_NORMALIZED = f"{PATH_SILVER}/votes_normalized"
    CHECKPOINT_SILVER_NORMALIZED = f"{CHECKPOINT_SILVER}/votes_normalized"

    # Gold layer
    PATH_GOLD = f"{PATH_BASE}/gold"
    CHECKPOINT_GOLD = f"{PATH_CHECKPOINT}/gold"

    CHECKPOINT_GOLD_VOTES_CLEAN = f"{CHECKPOINT_GOLD}/votes_clean"

    PATH_GOLD_PARTIES_PROVINCES_SEATS = f"{PATH_GOLD}/votes_parties_provinces_seats"
    CHECKPOINT_GOLD_PARTIES_PROVINCES_SEATS = (
        f"{CHECKPOINT_GOLD}/votes_parties_provinces_seats"
    )
