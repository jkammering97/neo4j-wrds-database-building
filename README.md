# Creating Company, ECC, Participant and Statement Nodes and Edges on Neo4J

This project builds a graph database of earnings conference call (ECC) data using metadata and transcript components from the WRDS Capital IQ dataset. 

Data flows through a local **PostgreSQL** database for normalization and indexing before being pushed into **Neo4j** for Master Data (Company and ECC) - [ECC/Company Data Process](#-ecc_company_datapy)

For Statement and Participant Data, JSON files are used for intermediate local storage before being uploaded to the graph database - [Statement/Participant Data Process](#-statement_participant_datapy)

---
## Overview of the System

- **Source:** WRDS CIQ Transcripts (via SQL queries)
- **Staging:** Local PostgreSQL (`ecc_pg_db`)
- **Graph Model:** Neo4j (hosted remotely at `triathlon.itit.gu.se`)
- **Key Entities:**
  - Company
  - ECC (Earnings Conference Call)
  - Participant (Speaker)
  - Statement (Spoken Text)

---

## 📁 `ecc_company_data.py`

**Goal:**  
Build master data for **Companies** and their **ECC events**, normalize it into PostgreSQL, and populate the base Neo4j graph.

### Execution Flow

1. **Connect to WRDS:**
   - Establishes a `wrds.Connection()` to the WRDS SQL environment.

2. **Fetch Company Metadata:**
   - Uses a multi-joined query from the following WRDS tables:
     - `ciq_transcripts.wrds_transcript_detail`: basing the companies table on this transcripts table seems plausible since statements will be drawn from the same table.
     - `ciq_common.ciqsymbol`: these are very messy, a good process to identify correct symbols could not be found, therefore its being queried for symb.activeflag = 1; shortest statement using ROW_NUMBER().
     - `ciq.ciqcompany`
     - `ciq.ciqcompanyindustrytree`
     - `ciq.ciqindustrytosic`
     - `ciq.ciqcountrygeo`
   - Filters for records after `2014-01-01`.
   - Uses `ROW_NUMBER()` to rank and deduplicate symbol values and industry tags.

3. **PostgreSQL – Company Table:**
   - Table: `company`
   - Columns:
     - `companyid`: WRDS unique identifier
     - `companyname`
     - `symbol`
     - `country`
     - `industry`
   - Creates table if not exists and inserts only unique companies.

4. **Fetch ECC Event Metadata:**
   - Queries `wrds_transcript_detail` to extract:
     - `keydevid`: Event ID
     - `companyid`
     - `headline`
     - `mostimportantdateutc` + `mostimportanttimeutc` (as `datetime_utc`)

5. **PostgreSQL – ECC Table:**
   - Table: `ecc`
   - Columns:
     - `keydevid`: Unique ECC identifier
     - `companyid`: Foreign Key to `company`
     - `title`
     - `datetime_utc`, `year`, `quarter`
   - Inserts ECC records per company if `companyid` exists in `company` table.

6. **Neo4j Insertion:**
   - Creates nodes:
     - `Company {companyid, name, symbol}`
     - `ECC {keydevid, title, time, quarter, year, symbol}`
     - `Country {name}`
     - `Industry {name}`
   - Relationships:
     - `(Company)-[:ARRANGED]->(ECC)`
     - `(Company)-[:IN_COUNTRY]->(Country)`
     - `(Company)-[:IN_INDUSTRY]->(Industry)`
   - Indexes created on `Company(companyid)` and `ECC(keydevid)`.

---

## 📁 `statement_participant_data.py`

**Goal:**  
Fetch **Statements** and **Participants** from WRDS per company, save as JSON, and create speaker–ECC and statement–ECC edges in Neo4j.

### Execution Flow

1. **Connect to PostgreSQL & WRDS:**
   - Loads `companyid`s from the local `company` table.
   - Establishes WRDS connection with fallback to password prompt.

2. **Query Transcript Data:**
   - For each company:
     - Gets transcript components from:
       - `wrds_transcript_detail`
       - `ciqtranscript`
       - `ciqtranscriptcomponent`
       - `wrds_transcript_person`
   - Applies a `ROW_NUMBER()` partition to drop duplicated join rows.

3. **Save to Local JSON:**
   - `batch.json`: Full statement list (one per transcript component).
   - `batch_participants.json`: (participant, ECC) pairs.
   - `batch_participants_unique.json`: Unique participant metadata.

4. **Upload to Neo4j:**
   - these are run for two different approaches: FIRST ITERATION & SECOND ITERATION
      - it was commented out which one was not run
      - the main difference is the following:
         - FIRST ITERATION: Nodes and Edges are inserted using Neo4j's CREATE
            - here they had 
   - Nodes:
     - `Statement {text, order, transcriptcomponentid}`
     - `Participant {name, type, transcriptpersonid}`
   - Edges:
     - `(Participant)-[:PARTICIPATED_IN]->(ECC)`
     - `(Statement)-[:WAS_GIVEN_AT]->(ECC)`

5. **Error Handling:**
   - Logs failures to `logs/failed_companies.txt`
   - Retries failures automatically in a second run.

---

## PostgreSQL Masterdata

Database: `ecc_pg_db`

### Table: `company`

| Column      | Type     | Description                        |
|-------------|----------|------------------------------------|
| id          | SERIAL   | Internal Primary Key               |
| companyid   | INTEGER  | Unique WRDS ID                     |
| companyname | TEXT     | Company name                       |
| symbol      | TEXT     | Ticker symbol                      |
| country     | TEXT     | Country of registration            |
| industry    | TEXT     | SIC-based industry classification  |

### Table: `ecc`

| Column       | Type         | Description                                 |
|--------------|--------------|---------------------------------------------|
| id           | SERIAL       | Internal Primary Key                        |
| keydevid     | BIGINT       | Unique event ID                             |
| companyid    | INTEGER      | Foreign Key to company                      |
| title        | TEXT         | Headline of the ECC                         |
| quarter      | INT          | Derived from timestamp                      |
| year         | INT          | Derived from timestamp                      |
| datetime_utc | TIMESTAMPTZ  | Date & time in UTC                          |

---

## Notes

- Neo4j index creation was partially done via browser; verify existence before running at scale.
- Due to high volume, `Statement` and `Participant` upload was executed via terminal for stability.
- JSON-based upload ensures decoupling between WRDS fetch and Neo4j ingestion.

---

## Running the Pipeline

### 1. Install requirements in virtual env
pip install -r requirements.txt

### 2. Setup `.env` File
`WRDS_USERNAME=your_wrds_username`

`WRDS_PASSWORD=your_wrds_password`

`POSTGRE_PASSWORD=your_pg_password`

`NEO4JEXTUSER=your_neo4j_user`

`NEO4JEXTPASS=your_neo4j_password`

### 3. Create Unique Constraints in Neo4j
these should be created before any data is pushed to the Neo4j database, since 
the CREATE functionality will only fail for duplicates when the constraints are in place.
This allows for fast upload in the two step approch (FIRST ITERATION, SECOND ITERATION)

**for ECCs:**

`CREATE CONSTRAINT ecc_keydevid_unique IF NOT EXISTS FOR (e:ECC) REQUIRE e.keydevid IS UNIQUE`

**for Statements:**

`CREATE CONSTRAINT c_transcriptcomponentid_unique IF NOT EXISTS FOR (s:Statement) REQUIRE s.c_transcriptcomponentid IS UNIQUE`

_this next constraint is only helpful for the later chunk creation when the id property is used for a MATCH lookup_

`CREATE CONSTRAINT statement_id_unique IF NOT EXISTS FOR (s:Statement) REQUIRE s.id IS UNIQUE`

**for Participants:**

`CREATE CONSTRAINT c_transcriptpersonid_unique IF NOT EXISTS FOR (p:Participant) REQUIRE p.c_transcriptpersonid IS UNIQUE`

### 4. Create and Post to Neo4j: ECC and Company Masterdata
run in interactive mode (https://code.visualstudio.com/docs/python/jupyter-support-py)

### 5. Create and Post to Neo4j: Statements and Participants
```bash
python statement_participant_data.py

### File structure

graph_builder/
├── ecc_company_data.py
├── statement_participant_data.py
├── logs/
│   ├── import_log_<timestamp>.txt
│   ├── failed_companies.txt
│   └── failed_companies_second_iteration.txt
├── local_int/
│   ├── batch.json
│   ├── batch_participants.json
│   └── batch_participants_unique.json
├── .env
└── README.md
