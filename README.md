# ARAR Collection 12 Scraper — Publications of the Institute of Literature after M. Abeghyan of NAS RA

This repository contains a Python scraper designed to extract **metadata from the Pan-Armenian Digital Library (ARAR)**.

The target source is:

**Publications of the Institute of Literature after M. Abeghyan of NAS RA**

The project is part of **Open Data Armenia**, an initiative that aims to collect, organize, and document **Armenian cultural heritage around the world**.

---

## Open Data Armenia

**Open Data Armenia** is a digital humanities initiative focused on building structured datasets about Armenian cultural heritage preserved in institutions worldwide.

The initiative aims to:

- document Armenian heritage across global archives, libraries, museums, and digital collections
- make Armenian cultural data more discoverable
- support digital humanities and historical research
- create reusable open datasets for scholars, students, and developers
- improve visibility of Armenian historical collections around the world

Repositories in this initiative collect **metadata only**, while original materials remain hosted by their custodial institutions and rights holders.

---

## Project Goals

This repository aims to:

- collect structured metadata about Armenian cultural heritage materials
- improve discoverability of Armenian heritage collections
- support digital humanities research
- document Armenian heritage preserved in global and Armenian digital collections

The project focuses on **metadata extraction only**, not copying or redistributing original materials.

---

## Data Source

**Platform:** Pan-Armenian Digital Library (ARAR)  
**Website:** https://arar.sci.am  
**Collection:** Publications of the Institute of Literature after M. Abeghyan of NAS RA  
**Collection URL:** https://arar.sci.am/dlibra/results?q=&action=SimpleSearchAction&type=-6&p=0&qf1=collections:16

ARAR contains hundreds of thousands of digitized objects, including:

- books
- manuscripts
- periodicals
- maps
- posters
- archival publications

---

## What the Scraper Does

The script:

1. accesses ARAR collection result pages
2. retrieves item links
3. visits each item page
4. extracts structured metadata
5. cleans and normalizes fields
6. exports structured datasets

The scraper collects **metadata only** and links back to the original records.

---

## Exported Data Fields

The dataset contains the following fields when available:

| Field | Description |
|------|-------------|
| id | Item identifier |
| publication_id | ARAR publication ID |
| edition_id | ARAR edition ID |
| title | Item title |
| author_creator | Author or creator |
| date_period | Publication date |
| year | Extracted year |
| place_of_publishing | Publication location |
| publisher | Publisher |
| language | Language |
| subject_keywords | Keywords |
| url_original_object | Link to the original record |

---

## Repository Structure

main.py  
arar_collection_16.csv  
arar_collection_16.jsonl  
README.md  
LICENSE  
DATA_RIGHTS.md  
requirements.txt  
.gitignore  
CITATION.cff  

---

## Installation

Clone the repository:

git clone REPOSITORY_URL

Create a virtual environment:

python -m venv .venv

Activate the environment:

source .venv/bin/activate

Install dependencies:

pip install -r requirements.txt

---

## Usage

Run the scraper:

python main.py

The script will automatically collect metadata and export:

arar_collection_16.csv  
arar_collection_16.jsonl

---

## Dataset

The repository includes structured datasets exported in:

- CSV format
- JSONL format

The dataset contains **metadata only** and links back to the original ARAR records.

---

## License

See the `LICENSE` file.

---

## Rights Notice

This repository is part of **Open Data Armenia**.

All copyrights to the original collection materials,
including books, manuscripts, scans, images, institutional descriptions,
and metadata records belong to **their respective owners and custodial institutions**.

This repository **does not claim ownership over the original materials**.

The project only collects and structures publicly available metadata.

Users wishing to reuse original materials should consult the source institution's policies and the relevant rights holders.

For additional details, see `DATA_RIGHTS.md`.

---

## Disclaimer

This project is an independent research effort.

It is **not officially affiliated with the Pan-Armenian Digital Library (ARAR)**, the **Institute of Literature after M. Abeghyan of NAS RA**, or any holding institution unless explicitly stated.

---

## Author

**Open Data Armenia**
