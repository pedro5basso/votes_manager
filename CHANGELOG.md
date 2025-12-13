# Changelog 
All notable changes to this project will be documented in this file. 

## [Unreleased]

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