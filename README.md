# Hercules

> An attempt to reproduce **Google File System (GFS, 2002)**

---

## Table of Contents

- [Overview](#overview)  
- [Goals](#goals)  
- [Architecture](#architecture)  
- [Components](#components)  
- [Getting Started](#getting-started)  
- [Usage](#usage)  
- [Experiments / Testing](#experiments--testing)  
- [Contributing](#contributing)  
- [License](#license)  
- [Acknowledgements](#acknowledgements)

---

## Overview

Hercules is a project aiming to *reproduce the design and behavior* of the Google File System (GFS), as described in the original 2002 paper. It includes implementations of the key components such as chunkservers, master server, namespace management, and failure detection to explore ideas around large-scale distributed file storage, fault tolerance, and scalability.

This project is written primarily in **Go** with some supporting Python code.

---

## Goals

- Re-create the core architecture and behavior of GFS:
  - Chunk storage and replication  
  - Metadata and namespace management  
  - Failure detection and recovery  
  - Client‐server interactions  
  - Master server responsibilities  
- Provide a platform for experimentation with distributed file system behavior.  
- Serve as a learning / teaching tool for systems and distributed computing.  
- (Optional) Allow visualisation / plotting of system metrics.  

---

## Architecture

Hercules is organized into several interacting services:

- **Master Server** — handles metadata, namespace, and coordination.  
- **Chunkservers** — storage nodes holding file chunks.  
- **Clients** — communicate with master for metadata, then with chunkservers for data transfers.  
- **Failure Detector** — monitors chunkservers and triggers re-replication on failures.  
- **Namespace Manager / Archive Manager** — manages file naming, directories, and archiving.  
- **Plotter** — provides visualisation and metrics.  

---

## Components

| Module | Description |
|---|---|
| `master_server` | Logic for the master metadata server. |
| `chunkserver` | The storage servers holding file chunks. |
| `client` / `tsclient` | Interfaces for file system clients. |
| `namespace_manager` | Management of directory structure / file naming. |
| `failure_detector` | Detect and handle server failures. |
| `archive_manager` | For snapshotting / archiving. |
| `plotter` | Visualisation and graphing of system behavior. |
| `common`, `utils`, `rpc_struct` | Shared utilities, data structures, RPC definitions. |
| `download_buffer` | Buffering mechanisms for streaming / data transfers. |

---

## Getting Started

### Requirements

- Go (>= 1.x)  
- Python (for supporting scripts)  
- Network access (for RPC between components)  
- (Optional) Tools for plotting / visualisation  

### Setup

```bash
# Clone the repo
git clone https://github.com/caleberi/hercules.git
cd hercules

# Build services
cd master_server
go build
# do the same for chunkserver, etc.

# Install Python requirements (if needed)
pip install -r requirements.txt
````

Configure ports, addresses, and replication factors in configs (if applicable).

---

## Usage

* Start the **master server**
* Start one or more **chunkservers**, pointing them to the master server
* Run **client** or test scripts to create files, read/write, or simulate failures
* Use **failure detector** to monitor failures and trigger recovery
* Optionally use **plotter** to view metrics

---

## Experiments / Testing

Try experimenting with:

* Varying replication factors
* Simulating network latency or chunk server downtime
* Measuring throughput with multiple clients
* Checking correctness and consistency after failures

---

## Contributing

Contributions are welcome!

* Fix bugs or improve correctness
* Add more tests or simulation scenarios
* Enhance visualisation or monitoring capabilities
* Optimise performance
* Extend to support additional features

Please use clear commit messages and document your changes.

---

## License

This project is released under MIT.

---

## Acknowledgements

* Inspired by *The Google File System* (2003) by Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung
* Thanks to all contributors
