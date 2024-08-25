# Visualization of MobilityDB Data in QGIS

This repository contains proof-of-concept implementations of the various designs explored in my master's thesis, titled **"Visualization of MobilityDB Data in QGIS."** The associated written report is available [here](master_thesis_ali_qgis_visualization.pdf).



![Interactive Mode visualization](Interactive_Mode/visuals/interactive_mode.gif)


## Overview

This repository showcases the experimental setups and implementations from my thesis work. The research focuses on enhancing the visualization of spatiotemporal data from MobilityDB within QGIS, a widely-used open-source Geographic Information System (GIS) platform. The thesis evaluates existing visualization techniques and tools, including OpenLayers, Deck.gl, and Leaflet, with a particular focus on leveraging the newly developed MEOS library. MEOS, an open-source C library for mobility data management, enables direct interaction with spatiotemporal objects and operations across various programming environments.

This work utilizes the Python bindings PyMEOS and PyQGIS to integrate MobilityDB data into QGIS, offering innovative solutions that build upon previous efforts like the MOVE plugin by Maxime Schoemans. A key contribution of this thesis is the design based on "Time Deltas," which divides the timeline into subsets, allowing for continuous and efficient animation of spatiotemporal data while minimizing memory usage. The thesis demonstrates how to use PyMEOS’s `value_at_timestamp` and `tsample` functions, along with PyQGIS’s capabilities, to develop practical QGIS plugins for enhanced visualization and animation of MobilityDB data.

The repository provides working demos for these experimentations(described in the Thesis report) :

- Interactive Mode
- TimeDeltas Mode
- Vector Tile Layer demo
- Resampling Mode : single-core and multi-core versions
- Vector Tile experiment

## Build Configuration

- **Operating System**: Ubuntu 22.04
- **Programming Language**: Python 3.11
- **Database**: PostgreSQL 16,  PostGIS 3.4.2, [MobilityDB](https://github.com/MobilityDB/MobilityDB)
- **Tools**: [QGIS 3.14 or newer](https://qgis.org/resources/installation-guide/?highlight=ubuntu), [PyMEOS](https://pymeos.readthedocs.io/en/latest/) 

## Usage

To get started with the provided proof-of-concept designs, ensure that all dependencies listed above are installed and properly configured on your system. Detailed instructions for each experiment can be found in their respective directories within this repository.
