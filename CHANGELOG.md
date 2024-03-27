# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Added
### Changed
### Deprecated
### Removed
### Fixed
- Fixed install of `kubectl` in Collection Manager image build
### Security

## [1.2.0] - 2023-11-22
### Added
- SDAP-477: Added preprocessor to properly shape incoming data
- SDAP-478: Add support to user to select subgroup of interest in input granules
### Changed
- Changed granule ingester setup to use mamba instead of conda (superseded by SDAP-511)
- SDAP-511: Switched package management to use Poetry instead of conda/mamba
### Deprecated
### Removed
- SDAP-501: Updated dependencies to remove `chardet`
### Fixed
- SDAP-488: Workaround to build issue on Apple Silicon (M1/M2). GI image build installs nexusproto through PyPI instead of building from source. A build arg `BUILD_NEXUSPROTO` was defined to allow building from source if desired/
### Security

## [1.1.0] - 2023-04-26
### Added
- SDAP-437: Added support for preprocessing of input granules. Initial implementation contains one preprocessor implementation for squeezing one or more dimensions to ensure the dataset is shaped as needed.
- SDAP-483: Added `.asf.yaml` to configure Jira auto-linking.
### Changed
### Deprecated
### Removed
### Fixed
- SDAP-423: Fixed verbosity settings not propagating to ingester subprocesses
- SDAP-417: Fixed bug where very spatially narrow tiles would have their WKT for the geo field represent the incorrect shape (ie a very narrow polygon being rounded to a line), which would cause an error on write to Solr.
- SDAP-435: Added case for handling time arrays of type `np.timedelta64`
### Security

## [1.0.0] - 2022-12-05
### Added
### Changed
 - SDAP-408: Improve L2 satellite data ingestion speed
   - Improved fault tolerance of writes to data & metadata stores. For ingestion pipelines that generate many tiles, the data stores may fail on some writes which was treated as an unrecoverable failure. Now more tolerant of this.
   - Batched writes: Reduced the number of network IO operations by consolidating writes of tile data + metadata.
   - Removed unnecessary function call. Removed an unneeded function call that seemed to be consuming a lot of pipeline runtime.
   - Batched tasks submitted to executors in pool. Saves wasted time switching between completed & new tasks.
- Added missing ASF headers to all .py files in this repository.
- Added ASF `README` for release.
### Deprecated
### Removed
### Fixed
### Security


