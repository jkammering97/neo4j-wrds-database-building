#%%
import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import psycopg2

import wrds

from neo4j import GraphDatabase
#%%
db = wrds.Connection()

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

def init_graph_DB():
	scheme = "bolt"
	host_name = "triathlon.itit.gu.se"
	port = 7688
	url = "{scheme}://{host_name}:{port}".format(scheme=scheme, host_name=host_name, port=port)
	user = os.getenv('NEO4JEXTUSER')
	password = os.getenv('NEO4JEXTPASS')

	driver = GraphDatabase.driver(url, auth=(user, password))

	return driver
#%%
# the symbol values here are not clean, select symbolvalue = 4 for clean ones,
#  this though misses a third of the data
get_all_companies_14 = """
                WITH all_companies_symbol AS (
                    SELECT
                        d.companyid
                        ,d.companyname
                        ,symb.symbolvalue
                        ,ROW_NUMBER() OVER (
                            PARTITION BY symb.relatedcompanyid 
                            ORDER BY 
                                LENGTH(symb.symbolvalue),  -- Shorter symbols are prioritized
                                symb.symbolvalue           -- Lexical order
                        ) AS symbol_rank
                    FROM ciq_transcripts.wrds_transcript_detail d
                        JOIN ciq_common.ciqsymbol AS symb ON d.companyid = symb.relatedcompanyid
                    WHERE d.mostimportantdateutc > '2014-01-01'
                    AND symb.activeflag = 1
                ),
                company_details AS (
                    SELECT 
                    w.companyid
                    ,w.companyname
                    ,comp.city
                    ,comp.companystatustypeid
                    ,geo.country
                    ,w.symbolvalue
                    ,sic.siccode AS industry_siccode
                    ,sic.sicdescription AS industry_sicdescription
                    ,ROW_NUMBER() OVER (
                        PARTITION BY w.companyid 
                        ORDER BY 
                            (CASE 
                                WHEN sic.siccode IS NOT NULL AND sic.sicdescription IS NOT NULL THEN 1 
                                ELSE 2 
                            END)
                    ) AS rn
                    FROM 
                        all_companies_symbol AS w
                        -- Join with ciqcompany for company-specific details
                        JOIN ciq.ciqcompany AS comp ON w.companyid = comp.companyid
                            JOIN ciq.ciqcountrygeo as geo ON comp.countryid = geo.countryid
                                JOIN ciq.ciqcompanyindustrytree AS ind ON w.companyid = ind.companyid
                                    -- Join with ciqindustrytosic to obtain SIC code using subtypeid
                                    JOIN ciq.ciqindustrytosic AS sic ON ind.subtypeid = sic.subtypeid
                    WHERE w.symbol_rank = 1
                )
                SELECT * 
                FROM company_details
                WHERE rn = 1;
                """
#%%
company_data_long = db.raw_sql(get_all_companies_14)
company_data_long.dropna(inplace=True)
#%% CREATE COMPANY TABLE
conn = connect_to_postgresql_db()
cur = conn.cursor()
cur.execute("""CREATE TABLE company (
            id SERIAL PRIMARY KEY,
            companyid INTEGER UNIQUE,
            companyname TEXT NOT NULL,
            symbol TEXT,
            country TEXT,
            industry TEXT);
            """)
conn.commit()
cur.close()
conn.close()
#%% INSERT COMPANIES
conn = connect_to_postgresql_db()
cur = conn.cursor()
company_data_long = company_data_long.astype(object).where(pd.notna(company_data_long), None)  # Convert NaNs to None
company_data_long['companyid'] = company_data_long['companyid'].astype('Int64')
company_data_long.dropna(inplace=True)
# Insert unique companies
unique_companies = company_data_long[['companyid', 'companyname','symbolvalue', 'country', 'industry_sicdescription']].drop_duplicates()
print(len(unique_companies))
for _, row in unique_companies.iterrows():
    cur.execute("""
    INSERT INTO company (companyid, companyname, symbol, country, industry) 
    VALUES (%s, %s,%s, %s,%s) 
    ON CONFLICT (companyid) DO NOTHING;
    """, (row['companyid'], row['companyname'],row['symbolvalue'],row.get('country', 'Unknown country'), row.get('industry_sicdescription', 'Unkown industry')))
    conn.commit()
conn.commit()
cur.close()
conn.close()
#%% GET ECC EVENTS
get_ecc_keydev = """
    SELECT DISTINCT ON (w.companyid, w.keydevid)
        w.companyid
        ,w.keydevid
        ,w.headline AS title
        ,(w.mostimportantdateutc || 'T' || w.mostimportanttimeutc || 'Z') AS datetime_utc
    FROM ciq_transcripts.wrds_transcript_detail AS w 
    WHERE w.mostimportantdateutc >= '2014-01-01';
    """
    # WHERE EXTRACT(YEAR FROM w.mostimportantdateutc) = 2020  -- Filter for 2020
    # ;
#%%
get_ecc_keydev_df = db.raw_sql(get_ecc_keydev)
#%%
def create_ecc_table_postgresql():
    conn = connect_to_postgresql_db()
    cur = conn.cursor()
    
    cur.execute("""CREATE TABLE ecc (
                            id SERIAL PRIMARY KEY
                            ,keydevid BIGINT UNIQUE -- Ensuring uniqueness for FK reference
                            ,companyid INTEGER REFERENCES company(companyid) ON DELETE CASCADE
                            ,title TEXT NOT NULL
                            ,quarter INT
                            ,year INT
                            ,datetime_utc TIMESTAMPTZ);
                """)

    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL table created successfully!")
#%%
create_ecc_table_postgresql()
#%%
def insert_ecc_data_postgresql(df):
    if 'companyid' not in df.columns:
        raise ValueError("Missing 'companyid' column in DataFrame!")

    conn = connect_to_postgresql_db()
    cur = conn.cursor()
    df['keydevid'] = pd.to_numeric(df['keydevid'], errors='coerce').dropna().astype('Int64')
    df = df.astype(object).where(pd.notna(df), None)  # Convert NaNs to None
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], utc=True)
    # Add year and quarter
    df['year'] = df['datetime_utc'].dt.year
    df['quarter'] = df['datetime_utc'].dt.quarter
    # Insert unique conferences
    for _, row in df.iterrows():
        print(f'row{_}out of {len(df)}')
       
        # Check if companyid exists in company table
        cur.execute("SELECT 1 FROM company WHERE companyid = %s;", (row['companyid'],))
        exists = cur.fetchone()

        if not exists:
            print(f"not in company table {row['companyid']}")
            continue
            
        cur.execute("""
        INSERT INTO ecc (keydevid, companyid, title, quarter, year, datetime_utc)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (keydevid) DO NOTHING;
        """, (row['keydevid'], row['companyid'], row['title'],row['quarter'],row['year'], row['datetime_utc']))
    conn.commit()
    cur.close()
    conn.close()
# %%
insert_ecc_data_postgresql(get_ecc_keydev_df)
#%%
driver = init_graph_DB()
# Function to create indexes in Neo4j
def create_indexes():
    with driver.session() as session:
        session.run("CREATE INDEX FOR (c:Company) ON (c.companyid);")
        session.run("CREATE INDEX FOR (e:ECC) ON (e.keydevid);")
        print("✅ Indexes created.")
        # constraints i created manually

# Function to fetch company data from PostgreSQL
def fetch_company_data():
    conn = connect_to_postgresql_db()
    query = "SELECT companyid, companyname, symbol, country, industry FROM company;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def insert_country_and_industry_nodes():
    companies = fetch_company_data()
    unique_countries = companies['country'].dropna().unique()
    unique_industries = companies['industry'].dropna().unique()

    with driver.session() as session:
        for country in unique_countries:
            session.run("""
            MERGE (:Country {name: $country})
            """, country=country)

        for industry in unique_industries:
            session.run("""
            MERGE (:Industry {name: $industry})
            """, industry=industry)

    print("✅ Inserted Country and Industry nodes.")

def create_country_industry_relationships():
    companies = fetch_company_data()

    with driver.session() as session:
        for _, row in companies.iterrows():
            print(f'industry_company_rel:{_}/{len(companies)}')
            session.run("""
            MATCH (c:Company {companyid: $companyid})
            MATCH (country:Country {name: $country})
            MERGE (c)-[:IN_COUNTRY]->(country)
            """, companyid=row['companyid'], country=row['country'])

            session.run("""
            MATCH (c:Company {companyid: $companyid})
            MATCH (industry:Industry {name: $industry})
            MERGE (c)-[:IN_INDUSTRY]->(industry)
            """, companyid=row['companyid'], industry=row['industry'])

    print("✅ Created relationships from Company to Country and Industry.")

# Function to fetch ECC data from PostgreSQL
def fetch_ecc_data():
    conn = connect_to_postgresql_db()
    query = """
            SELECT 
                e.keydevid 
                ,e.companyid
                ,e.title
                ,e.quarter
                ,e.year
                ,e.datetime_utc 
                ,c.symbol
            FROM ecc e
            JOIN company c ON c.companyid = e.companyid;
            """
    df = pd.read_sql(query, conn)
    # Convert keydevid safely
    df['keydevid'] = pd.to_numeric(df['keydevid'], errors='coerce').dropna().astype('Int64')
    # Convert to datetime if not already
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], errors='coerce')
    
    conn.close()
    return df

# Function to insert Company nodes into Neo4j
def insert_company_data():
    companies = fetch_company_data()
    with driver.session() as session:
        for _, row in companies.iterrows():
            print(f'companies:{_}/{len(companies)}')
            session.run("""
            MERGE (c:Company {companyid: $companyid})
            SET 
                c.name = $companyname,
                c.symbol = $symbol;
            """, companyid=row['companyid'],
            companyname=row['companyname'],
            symbol=row['symbol'])
    print(f"✅ Inserted {len(companies)} Company nodes.")
    insert_country_and_industry_nodes()
    create_country_industry_relationships()

# Function to insert ECC nodes into Neo4j
def insert_ecc_data_neo():
    eccs = fetch_ecc_data()
    with driver.session() as session:
        for _, row in eccs.iterrows():
            print(f'eccs:{_}/{len(eccs)}')
            session.run("""
            MERGE (e:ECC {keydevid: $keydevid})
            SET 
                e.title = $title, 
                e.time = $time,
                e.quarter = $quarter,
                e.year = $year,
                e.symobl = $symbol;
            """, keydevid=row['keydevid'],
                title=row['title'],
                time=row['datetime_utc'],
                quarter=row['quarter'],
                year=row['year'],
                symbol=row['symbol'],
                
                )
    print(f"✅ Inserted {len(eccs)} ECC nodes.")
#%%
# Function to create relationships (ECC → Company)
def create_relationships():
    eccs = fetch_ecc_data()
    with driver.session() as session:
        for _, row in eccs.iterrows():
            print(f'relationships:{_}/{len(eccs)}')
            session.run("""
            MATCH (c:Company {companyid: $companyid})
            MATCH (e:ECC {keydevid: $keydevid})
            MERGE (c)-[:ARRANGED]->(e);
            """, keydevid=row['keydevid'], companyid=row['companyid'])
    print("✅ Relationships created between ECC and Company.")
#%%
create_indexes()
insert_company_data()
insert_ecc_data_neo()
create_relationships()  

# Close Neo4j connection
driver.close()

# %%
