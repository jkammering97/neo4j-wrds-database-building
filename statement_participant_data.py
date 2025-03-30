import os
from dotenv import load_dotenv
load_dotenv(dotenv_path="/Users/joey/Desktop/uni/Master/graph_builder/.env")

import json
import logging
import psycopg2
from datetime import datetime
import pandas as pd
from more_itertools import chunked

import wrds

from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError, Neo4jError


def get_wrds_connection():
    try:
        return wrds.Connection()
    except Exception as e:
        print("⚠️ WRDS auto login failed. Falling back to manual password entry.")
        wrds_user = os.getenv('WRDS_USERNAME')
        wrds_pass = os.getenv("WRDS_PASSWORD")
        if not wrds_pass:
            import getpass
            wrds_pass = getpass.getpass("Enter your WRDS password: ")
        return wrds.Connection(wrds_username=wrds_user, wrds_password=wrds_pass)

# Logging:
log_filename = f"/Users/joey/Desktop/uni/Master/graph_builder/logs/import_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
failed_companies_log = "/Users/joey/Desktop/uni/Master/graph_builder/logs/failed_companies.txt"
failed_companies_log_second = "/Users/joey/Desktop/uni/Master/graph_builder/logs/failed_companies_second_iteration.txt"

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

# PostgreSQL connection settings
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "ecc_pg_db"
PG_USER = "joey"
PG_PASSWORD =os.getenv('POSTGRE_PASSWORD')

def connect_to_postgresql_db():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

NEO4JEXTUSER = os.getenv('NEO4JEXTUSER')
NEO4JEXTPASS = os.getenv('NEO4JEXTPASS')
def init_graph_DB():
    scheme = "bolt"
    host_name = "triathlon.itit.gu.se"
    port = 7688
    url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
    user = NEO4JEXTUSER
    password = NEO4JEXTPASS

    driver = GraphDatabase.driver(url, auth=(user, password))

    return driver

class WRDSFetcher:
    """
    WRDSFetcher handles retrieval and preparation of WRDS (Wharton Research Data Services) transcript data for a given company ID.
 
    :param company_id: Unique identifier for the company whose transcript data is being fetched.
    :type company_id: int
    :param wrds_db: An active connection to the WRDS database.
    :type wrds_db: wrds.Connection
 
    :ivar company_id: Unique identifier for the company whose transcript data is being fetched.
    :ivar wrds_db: An active connection to the WRDS database (Wharton Research Data Services)
    :ivar import_path: Local file path where the fetched data will be stored in JSON format (batch, batch_participants, batch_unique_participants).
 
    :raises ValueError: If no data is returned from the query.
 
    .. method:: get_wrds_data()
 
        Executes a SQL query to fetch and rank transcript component data for the specified company.
 
        - Filters transcripts starting from 2014-01-01.
        - Includes speaker names, types, and component texts.
        - query: ROW_NUMBER() partitions by keydevid and componentorder, these are returned as duplicates due to JOIN, this is filtered in the final SELECT using rn = 1
        - Removes duplicate transcript components.
        - Saves the complete data and separate participant-related data to JSON files:
            - ``batch.json``: All transcript components.
            - ``batch_participants.json``: Unique participant and event combinations.
            - ``batch_participants_unique.json``: Unique participants only.
        - Logs the data-saving process and handles cases where no data is returned.
 
    **Usage Example**::
 
        fetcher = WRDSFetcher(company_id=12345, wrds_db=wrds_conn)
        fetcher.get_wrds_data()
    """
    
    def __init__(self, company_id: int, wrds_db: wrds.Connection):
        self.company_id = company_id
        self.wrds_db = wrds_db
        self.import_path = os.path.expanduser("/Users/joey/Desktop/uni/Master/graph_builder/local_int/")

    def get_wrds_data(self):
        """this method handles the WRDS querying for one single company as well
        as the cleaning of that data
        it then saves the data into three separate JSON files for upload to Neo4j

        - ``batch.json``: All transcript components/Statements.
        - ``batch_participants.json``: Unique participant -> ECC-event combinations.
        - ``batch_participants_unique.json``: Unique participants only.

        :return: None
        """

        query = f"""
        WITH company_subset AS (
            SELECT transcriptid, CAST(keydevid AS VARCHAR) AS keydevid, companyid
            FROM ciq_transcripts.wrds_transcript_detail
            WHERE companyid = {self.company_id}
            AND mostimportantdateutc >= '2014-01-01'
        ),
        ranked_transcripts AS (
            SELECT
                w.companyid
                ,w.keydevid
                ,w.transcriptid
                ,c.componentorder AS c_componentorder
                ,c.transcriptcomponentid AS c_transcriptcomponentid
                ,c.transcriptid AS c_transcriptid
                ,c.transcriptpersonid AS c_transcriptpersonid
                ,p.transcriptpersonname
                ,p.speakertypename
                ,c.componenttext
                ,ROW_NUMBER() OVER (
                    PARTITION BY w.keydevid, c.componentorder
                    ORDER BY w.transcriptid DESC
                ) AS rn
            FROM 
                company_subset w
            JOIN 
                ciq_transcripts.ciqtranscript AS t ON w.transcriptid = t.transcriptid
            JOIN 
                ciq_transcripts.ciqtranscriptcomponent AS c ON t.transcriptid = c.transcriptid
            JOIN 
                ciq_transcripts.wrds_transcript_person AS p ON c.transcriptcomponentid = p.transcriptcomponentid
        )
        SELECT * FROM ranked_transcripts
        WHERE rn = 1
        ORDER BY transcriptid ASC, c_transcriptcomponentid ASC, c_componentorder ASC
        ;
        """
        df = self.wrds_db.raw_sql(query)
        
        if df.empty:
            logging.info(f"No data returned for company {self.company_id}")
            raise ValueError("No data returned.")
        
        df["c_transcriptpersonid"] = df["c_transcriptpersonid"].astype('Int64')
        df["keydevid"] = df["keydevid"].astype('Int64')
        duplicate_count = df.duplicated(subset=["c_transcriptcomponentid"]).sum()
        if duplicate_count > 0:
            logging.info(f"Found and dropping {duplicate_count} duplicate transcript components.")
        df = df.drop_duplicates(subset=["c_transcriptcomponentid"])
        
        json_filename = f"batch.json"
        full_path = os.path.join(self.import_path, json_filename)
        df.to_json(full_path, orient="records", lines=True, force_ascii=False)

        # filter duplicates out in the results df
        participants_df = df[[
            "c_transcriptpersonid", "transcriptpersonname", "speakertypename", "keydevid"
        ]].dropna(subset=["c_transcriptpersonid", "transcriptpersonname", "speakertypename"])


        participants_df_unique = participants_df.drop_duplicates(subset=["c_transcriptpersonid"])

        # duplicates would be a row not unique by ECC (keydevid) and Statement (c_transcriptpersonid)
        participants_df = participants_df.drop_duplicates(subset=["c_transcriptpersonid", "keydevid"])
 
        full_path_participants = os.path.join(self.import_path, f"batch_participants.json")
        participants_df.to_json(full_path_participants, orient="records", lines=True, force_ascii=False)
 
        # generate the unique participants from the filtered participants_df
        participants_df_unique = participants_df.drop_duplicates(subset=["c_transcriptpersonid"])

        full_path_participants_unique = os.path.join(self.import_path, f"batch_participants_unique.json")
        participants_df_unique.to_json(full_path_participants_unique, orient="records", lines=True, force_ascii=False)
        
        # Debug: Check if file was saved
        if os.path.exists(full_path):
            logging.info(f"File saved: {full_path}")
        else:
            logging.error(f"File not found after saving: {full_path}")

class Neo4jUploader:
    """
    Neo4jUploader uploads transcript and participant data into a Neo4j graph database.

    :param driver: An active Neo4j driver instance.
    :type driver: neo4j.GraphDatabase.driver

    :ivar import_path: Path to the directory containing the JSON files.
    :ivar full_path: Path to the JSON file with transcript components. 
    :ivar full_path_participants: Path to the JSON file with participant-event combinations. 
    :ivar full_path_participants_unique: Path to the JSON file with unique participants.

    .. method:: upload_to_neo4j()

        - Handles local files for participants and Statements
        - Uploads participant and statement nodes into Neo4j from local JSON files.

    .. method:: create_edges()

        Creates relationships in Neo4j:
            - PARTICIPATED_IN (Participant → ECC)
            - WAS_GIVEN_AT (Statement → ECC)
    """
    def __init__(self, driver):
        self.driver = driver
        self.import_path = os.path.expanduser("/Users/joey/Desktop/uni/Master/graph_builder/local_int/")

        # This is the Statement data separate from the Participant data.
        # For the Statement Nodes and Edges to ECC
        self.json_filename = f"batch.json"
        self.full_path = os.path.join(self.import_path, self.json_filename)

        # These are participants unique by c_transcriptpersonid and keydevid.
        # For the Participant-Ecc Edges
        self.json_filename_participants = f"batch_participants.json"
        self.full_path_participants = os.path.join(self.import_path, self.json_filename_participants)

        # These are participants unique by c_transcriptpersonid.
        # for the Participant Nodes
        self.json_filename_unique_participants = f"batch_participants_unique.json"
        self.full_path_participants_unique = os.path.join(self.import_path, self.json_filename_unique_participants)

    def upload_to_neo4j(self):
        """this method handles the file structure locally (assigns) and uploads Nodes to Neo4j
        - uses neo4J native transaction method execute_write() https://neo4j.com/docs/python-manual/current/transactions/

        :loads to class instance:
        - ``data`` from ``batch.json``: All transcript components/Statements.
        - ``participant_data`` from ``batch_participants.json``: Unique participant -> ECC-event combinations.
        - ``participants_unique`` from ``batch_participants_unique.json``: Unique participants only.

        :return: None
        """

        # local file for Statements
        with open(self.full_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            data = [json.loads(line) for line in lines]
        self.data = data
        print(f"uploading {len(data)} records from {self.json_filename} to Neo4j")
        
        # Local file for non-uniqe participants (for edges with ECC)
        with open(self.full_path_participants, "r", encoding="utf-8") as f:
            lines = f.readlines()
            participants = [json.loads(line) for line in lines]
        self.participant_data = participants

        # local file for unique Participants (Participant Nodes)
        with open(self.full_path_participants_unique, "r", encoding="utf-8") as f:
            lines = f.readlines()
            participants_unique = [json.loads(line) for line in lines]
        self.participants_unique = participants_unique

        with self.driver.session() as session:
            # defining the transaction like this ensures atomic transactions with the neo4j db
            # particularly when chunking or batching this is important since we could have duplicates 
            # between chunks logically or overwrite certain data
            def write_tx(tx, data, unique_participants):
                # chunking the upload data to not overload the db processes
                # often large batches took exceeding time
                chunk_size = 2000

                try:

                    for chunk in chunked(unique_participants,chunk_size):
                        tx.run("""
                            UNWIND $rows AS row
                            MERGE (p:Participant { c_transcriptpersonid: toInteger(row.c_transcriptpersonid) })
                            SET p.name = row.transcriptpersonname,
                                p.description = row.speakertypename
                        """, rows=chunk)

                        print(f"[{os.getpid()}] Processed {len(unique_participants)} participants-nodes.")
                        logging.info(f"[{os.getpid()}] Processing {len(unique_participants)} participants-nodes.")

                except ConstraintError as ce:
                    print(f"constraint violation (nodecreation_participant){ce}")
                    logging.error(f"constraint violation (nodecreation_participant){ce}")
                except Neo4jError as ne:
                    print(f"Neo4j nodecreation_participant failed: {ne.code} – {ne.message}")
                    logging.error(f"Neo4j nodecreation_participant failed: {ne.code} – {ne.message}")
                except Exception as e:
                    logging.error(f"Failed to insert nodecreation_participant\nError: {e}")

                try:
                    for chunk in chunked(data,chunk_size):
                        # SECOND ITERATION 
                        tx.run("""
                            UNWIND $rows AS row
                            MERGE (s:Statement { c_transcriptcomponentid: toInteger(row.c_transcriptcomponentid) })
                            SET s.text = row.componenttext,
                                s.name = row.transcriptpersonname,
                                s.order = toInteger(row.c_componentorder)
                        """, rows=chunk)

                        # FIRST ITERATION

                        # tx.run("""
                        #     UNWIND $rows AS row
                        #     CREATE (s:Statement {
                        #         c_transcriptcomponentid: toInteger(row.c_transcriptcomponentid),
                        #         text: row.componenttext,
                        #         name: row.transcriptpersonname,
                        #         order: toInteger(row.c_componentorder)})
                        # """, rows=chunk)

                        print(f"[{os.getpid()}] Processed {len(data)} statements-nodes.")
                        logging.info(f"[{os.getpid()}] Processed {len(data)} statements-nodes.")

                except ConstraintError as ce:
                    print(f"constraint violation (nodecreation-statement)")
                    logging.error(f"constraint violation (nodecreation-statement): {ce}")
                except Neo4jError as ne:
                    print(f"Neo4j transaction failed: {ne.code} – {ne.message}")
                    logging.error(f"Neo4j statement transaction failed: {ne.code} – {ne.message}")
                except Exception as e:
                    logging.error(f"Failed to insert node\nError: {e}")

            # this is the neo4j transaction function that handles the transaction
            # session passes the db instance and
            session.execute_write(write_tx, self.data, self.unique_participants)
        print(f"finished uploading {self.json_filename}")

    def create_edges(self):
        """this method creates the edges for Statement and Participant nodes, using
        also the native execute_write() function from Neo4j

        :return: None
        """
        with self.driver.session() as session:
            print(f"Uploading {self.json_filename} to Neo4j - Creating edges")

            def edge_tx(tx, data, participants):
                try:
                    # SECOND ITERATION
                    tx.run("""
                        UNWIND $rows AS row
                        MATCH (e:ECC {keydevid: row.keydevid})
                        MATCH (p:Participant {c_transcriptpersonid: toInteger(row.c_transcriptpersonid)})
                        MERGE (p)-[:PARTICIPATED_IN]->(e)
                        """, rows=participants)
                    
                    # FIRST ITERATION

                    # tx.run("""
                    #     UNWIND $rows AS row
                    #     MATCH (e:ECC {keydevid: row.keydevid})
                    #     MATCH (p:Participant {c_transcriptpersonid: toInteger(row.c_transcriptpersonid)})
                    #     CREATE (p)-[:PARTICIPATED_IN]->(e)
                    #     """, rows=participants)
                    print(f"[{os.getpid()}] Processed {len(participants)} participants.")
                    logging.info(f"[{os.getpid()}] Processed {len(participants)} participants.")
                 
                    # SECOND ITERATION
                    tx.run("""
                        UNWIND $rows AS row
                        MATCH (e:ECC {keydevid: row.keydevid})
                        MATCH (s:Statement {c_transcriptcomponentid: toInteger(row.c_transcriptcomponentid)})
                        MERGE (s)-[:WAS_GIVEN_AT]->(e)
                    """, rows=data)

                    # FIRST ITERATION

                    # tx.run("""
                    #     UNWIND $rows AS row
                    #     MATCH (e:ECC {keydevid: row.keydevid})
                    #     MATCH (s:Statement {c_transcriptcomponentid: toInteger(row.c_transcriptcomponentid)})
                    #     CREATE (s)-[:WAS_GIVEN_AT]->(e)
                    # """, rows=data)
                    print(f"[{os.getpid()}] Processed {len(data)} statements.")
                    logging.info(f"[{os.getpid()}] Processed {len(data)} statements.")
                    
                except ConstraintError as ce:
                    print(f"constraint violation (edgecreation) for row: {row['keydevid']}\nNeo4j Error: {ce}")
                    logging.error(f"constraint violation (edgecreation) for row: {row['keydevid']}\nNeo4j Error: {ce}")
                except Neo4jError as ne:
                    print(f"Neo4j transaction failed: {ne.code} – {ne.message}")
                    logging.error(f"Neo4j transaction failed: {ne.code} – {ne.message}")
                except Exception as e:
                    print(f"Failed to create edge for row: {row}\nError: {e}")
                    logging.error(f"Failed to create edge for row: {row}\nError: {e}")

            session.execute_write(edge_tx, self.data, self.participant_data)
        print(f"finished edgecreation {self.json_filename}")
   
class CompanyMetadataHandler:
    """
    CompanyMetadataHandler handles retrieval and insertion of company metadata from a PostgreSQL database.

    .. method:: fetch_company_data(second_iteration=False, companyid=1)

        Fetches company metadata from the PostgreSQL database.

    :param second_iteration: Whether to fetch a single company by ID (used for retrying failed companies).
    :type second_iteration: bool
    :param companyid: ID of the company to fetch.
    :type companyid: int
    :return: DataFrame with company metadata.
    :rtype: pandas.DataFrame
    """

    def fetch_company_data(self, second_iteration = False, companyid:int=1):
        """
        Fetches company metadata from the PostgreSQL database. for either all companies 
        starting from a certain companyid or company by company (when iterating over the failed companies list)
        
        :param second_iteration: Whether to fetch a single company by ID (used for retrying failed companies).
        :type second_iteration: bool
        :param companyid: ID of the company to fetch.
        :type companyid: int
        :return: DataFrame with company metadata.
        :rtype: pandas.DataFrame
        """
        if second_iteration:
            conn = connect_to_postgresql_db()
            query = f"SELECT companyid, companyname, country, industry FROM company WHERE companyid = {companyid}"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        else:
            conn = connect_to_postgresql_db()
            query = f"SELECT companyid, companyname, country, industry FROM company WHERE companyid > {companyid} ORDER BY companyid;"
            df = pd.read_sql(query, conn)
            conn.close()
            return df

def process_company(row: pd.Series,wrds_db: wrds.Connection):
    """this function handles the execution of the classes WRDSFetcher and Neo4jUploader
    for one company at a time

    :param row: company with company metadata
    :type row: pd.Series
    :param wrds_db: An active connection to the WRDS database (Wharton Research Data Services)
    :type wrds_db: wrds.Connection
    """
    companyid = row["companyid"]
    companyname = row["companyname"]
    wrds_fetcher = WRDSFetcher(companyid, wrds_db)
    driver = init_graph_DB()
    neo4j_uploader = Neo4jUploader(driver)

    try:
        print(f"[Company {companyname},{companyid}] ➜ Fetching")
        wrds_fetcher.get_wrds_data()
        neo4j_uploader.upload_to_neo4j()
        neo4j_uploader.create_edges()

    except ValueError as ve:
        logging.warning(f"No data returned for company {companyid}: {ve}")
    except Exception as e:
        logging.error(f"❌ Error processing company {companyid}: {e}")
        print(f"⚠️ Skipping company {companyid} due to error: {e}")
        with open(failed_companies_log_second, "a") as f:
            f.write(f"{companyid}\n")
    finally:
        logging.info(f"full_batch for company {companyid}")
        driver.close()

if __name__ == "__main__":
# SECOND ITERATION
    wrds_db = wrds.Connection()
    company_metadata_handler = CompanyMetadataHandler()
    failed_companies_log = "/Users/joey/Desktop/uni/Master/graph_builder/logs/failed_companies.txt"
    
    if os.path.exists(failed_companies_log):
            with open(failed_companies_log, "r") as f:
                failed_ids = [int(line.strip()) for line in f if line.strip().isdigit()]
    for i,companyid in enumerate(failed_ids):
        df = company_metadata_handler.fetch_company_data(second_iteration=True, companyid=companyid)
        for _, row in df.iterrows():
            process_company(row, wrds_db)
# FIRST ITERATION
    
    company_metadata_handler = CompanyMetadataHandler()
    last_processed_id = 1452296 # set last company that was processed
    companies = company_metadata_handler.fetch_company_data(last_processed_id)

    for _,row in companies.iterrows():
        process_company(row,wrds_db)
