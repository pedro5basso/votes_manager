# Changelog 
All notable changes to this project will be documented in this file. 

## [Unreleased]

## [0.9.0] - 2026-01-10
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