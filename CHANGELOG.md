# Changelog 
All notable changes to this project will be documented in this file. 

## [Unreleased]

## [1.4.0] - 2026-02-04
### Added
- folder images/ with project screenshots
### Changed
- readme.md file: adding information and images
### Removed

## [1.4.0] - 2026-02-02
### Added
- generation/coordinates/coordinates.py: docstrings on some functions
### Changed
- streaming/jobs/votes_streaming_job.py: adding regions on seats computations
### Removed

## [1.2.0] - 2026-02-02
### Added
- generation/coordinates/coordinates.py: adding coordinates from main cities from another repository.
- generation/coordinates/files/**.json: adding main city coordinates
### Changed
- fixed problem on showing votes on Kenya
- generation/votes_generator/vote_generator.py: updating dafault votes  to 20000
### Removed


## [1.1.0] - 2026-01-25
### Added
- generation/coordinates/coordinates.py: script to manage votes coordinates based on municipality coordinates
from Spain
- generation/coordinates/__init__.py
- generation/coordinates/files/*.json: coordinates from a province. Each file name corresponds to the province code
- docker-compose.yaml: 
  - adding healthcheck on schema-registry image
  - adding condition on console image to make sure all images started correctly
### Changed
- generation/votes_generator/vote_generator.py: now the vote coordination comes from the coordinates.py file
### Removed


## [1.0.0] - 2026-01-24
### Added
- /.idea folder files
- logs folder on docker compose volumes
- streaming/pymodules/requirements.txt: adding mysql-connector-python and python-dotenv modules
### Changed
- .gitignore:
  - removing old logs folders unused
  - removing /.idea folder
- db .env file, adding two hosts:
  - localhost for managing local generation votes
  - dockerhost for spark job access
- Some code beauty params for compatibility with python 3.8 and 3.11 (ex: dict -> Dict)
### Removed
- generation/votes_generator/vote_generator.py: deleting argument GENERATE_CSV_FILE from class VoteConfiguration
- All provinces with their seats hardcoded on spark job, now is able to access to the database.
- streaming/pymodules/Dockerfile: removing COPY votes.csv line, no longer used.

## [0.12.0] - 2026-01-23
### Added
- folder logs/ for managing all logs from a main file. Added to .gitignore
- logs/logging_config.py: script for managing loggings moved to this new folder. Adding also code  beauty
- folder commands for managing all configuration commands that the project require:
  - elasticsearch/: connectors commands and indexes commands
  - spark/: spark job run command
### Changed
- .gitignore: log file and logs pycache
- all imports related with the log generator script
- docstrings and code beauty on generation/db/database_connector.py
### Removed
- env.example: removing scala, spark and elasticsearch versions as are not used.
- generation/utils/logging_config.py: moved to logs folder
- generation/utils/logs/votes_generator.log: moved to .gitignore file


## [0.11.0] - 2026-01-23
### Added

### Changed
- code beauty on all python scripts. Adding docstrings, types on function arguments and returns.
Using libs isort, black and flake8 for code beauty.
- streaming/job/new_votes_streaming_job.py: renamed to streaming/jobs/votes_streaming_job.py
### Removed
- .gitignore: testing files
- streaming/jobs/votes_streaming_legacy.py

## [0.10.0] - 2026-01-21
### Added
- .gitignore:
  - mnt/ folder as a new folder for datalake and spark checkpoints
- generation/utils/kafka.py: New topics added. Adapting method for creating all topics at once. Updating few topics and 
producer configuration
- generation/utils/political_parties.py: new script for managing political parties info and being easier to adapt. All 
Political parties includes now popularity.
- streaming/connect/sink-elasticsearch-seats.json: connector for seats topics
- streaming/datalake/datalake_configuration.py: script for managing datalake and checkpoint locations
### Changed
- .gitignore: 
  - removing old datalake and checkpoints folder.
  - removing streaming.zip file used to run spark on old project versions
  - logs files as it will be managed in future on a single file
- docker-compose.yaml:
  - limiting memory of some images
  - Adding paths to spark checkpoints and datalake on volumes section
  - removing one spark worker
- generation/db/get_db_information.py: now political parties will be on a separate script
- generation/votes_generator/vote_generator.py: adding political party popularity logic when generating votes
- streaming/connect/Dockerfile: adding sink-elasticsearch-seats.json connector
- streaming/connect/sink-elasticsearch-votes.json: adapting connector to votes topic
- streaming/jobs/new_votes_streaming_job.py: the job now computes seats. The logic has been adapted to a python class.
- streaming/jobs/run_job.py: now running new spark job
- streaming/pymodules/Dockerfile: adding csv votes for testing operations with spark inside docker
- streaming/schemas/vote_schema.py: adding new schema for votes_normalized
### Removed


## [0.9.0] - 2026-01-13
### Added
- generation/utils/generate_topics.py: new python script for creating all kafka topics related
- streaming/connect/Dockerfile: file for connect elasticsearch with kafka
- streaming/connect/sink-elasticsearch-votes.json: Elasticsearch sink connector
### Changed
- docker-compose.yaml:
  - connect container: 
    - adding dependency on elasticsearch image.
    - adding CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE, CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE, CONNECT_PLUGIN_PATH on environment
  - elasticsearch conatiner:
    - adding node.name=elastic on envirnoment
- generation/utils/kafka.py: adapting logic to new script generate_topics.py
- streaming/jobs/new_votes_streaming_job.py: updating logic to connect with Elasticsearch
  
### Removed

## [0.8.0] - 2026-01-10
### Added
- Spark will be executed from docker container
- /mnt folder as a temporally checpoint location 
- spark version and elasticsearch version on .env file. Maybe will be removed in future as now the version is installed on docker container and not locally
- file generation/db/get_db_information.py: for managing db information and be able to be accessible in one python object
- generation/utils/boundary_objects.py: New atributes on class AutonomousRegion, new classes: Country, PoliticalParty and Parties for managing database information
- file generation/utils/kafka.py. Script for managing Kafka utilities and configuration
- connect/Dockerfile: docker file for connect image: install elasticsearch connector plugin
- pymodules/Dockerfile: docker file for spark configuration. Util for install pymodules on docker image.
- pymodules/requirements.txt: requirements file for add pymodules to be installed on docker container
- streaming/jobs/new_votes_streaming: script for running the spark job from the docker image and not locally. In a future version it will replace the current "votes_streaming_job.py" file.
 It is not yet connected to Elasticsearch due to several technic issues.
### Changed
- docker-compose.yaml: new images structure:
  - Adding spark images: for launching, master and workers (one master and two workers)
  - Kafka: controller and broker (for now one broker and one controller)
  - schema registry is now working
  - console will be redpanda image
  - connect image. Maybe remove in future as elasticsearch connector plugin is failing
  - network name: "votes" for "bigdata"
- Renaming "python" folder name for "generation"
- generation/votes_generator/vote_generator.py: script adapted to new classes and scripts implemented
- streaming/schemas/vote_schema.py: adding iso_3166_2 code for province and region
### Removed
- confluent/control-center image for topic gui use
- zookeper image, now using Kraft

## [0.7.0] - 2025-12-30
### Added
- /streaming/jobs/run_job.py: file for running spark job
- /streaming/jobs/votes_streaming_job.py: spark job for reading votes from kafka
- Scala and Spark version on env example file
- Adding on .gitignore datalake and checkpoints folders
### Changed
- /python/utils/boundary_objects.py: Removing fields from Autonomous Region class. Updating names on Province class
- /python/votes_generator/vote_generator.py: new configuration fields on VoteConfiguration class. 
Adding autonomous region and province name on default votes
### Removed

## [0.6.0] - 2025-12-14
### Added
- new file /spark/schemas/vote_schema.py: schema for managing votes
- new file /spark/jobs/votes_streaming_job.py: job for reading votes on kafka
### Changed
- Adding to .gitignore spark.zip file
### Removed

## [0.5.0] - 2025-12-13
### Added
- /python/utils/logging_config.py: adding new file for using and managing logs
### Changed
- .gitignore: adding new logs file and folder
- docker-compose.yaml: adding kafka configuration to upload messages
- /python/db/database_connector.py: adding logs
- /python/votes_generator/run.py: updating logs
- /python/votes_generator/vote_generator.py: adding logs, kafka connection and removing mysql for generic word 'database'
### Removed


## [0.4.1] - 2025-12-13
### Added

### Changed

### Removed
- /python/.env file from git repo

## [0.4.0] - 2025-12-13
### Added
- /python/.env.example file for database connection
- /python/db/database_connector.py: file used for database connection
- /python/utils/boundary_objects.py: file used for managing boundary objects, such as provinces
- /python/votes_generator/run.py: main file to run the vote generator
- /python/votes_generator/vote_generator.py: file to generate the votes
### Changed
- .gitignore file: adding pycache files from all python packages, adding .env file for database connection

### Removed

## [0.3.0] - 2025-11-24
### Added
- docker file 'docker-compose-yaml' with containers
### Changed

### Removed

## [0.2.0] - 2025-11-19
### Added
- Python package 'votes_generator'.
- Python file ./votes_generator/__init__.py
- Python file ./votes_generator/vote_generator.py
- virtual environment '.venv', set on '.gitignore'
### Changed
- Python file ./votes_generator/vote_generator.py: first script version
### Removed

## [0.1.0] - 2025-11-19
### Added
- Files 'Makefile' and 'CHANGELOG.md'.
### Changed
- File 'README.md'
### Removed