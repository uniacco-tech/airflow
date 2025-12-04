#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import logging
import redshift_connector

REDSHIFT_HOST = "redshift-cluster-demo.c9z6ctv4aehd.ap-south-1.redshift.amazonaws.com"
REDSHIFT_DB = "dev"
REDSHIFT_USER = "nishant"
REDSHIFT_PASSWORD = "dw620LFVkx1Y"
REDSHIFT_PORT = 5439

def run_query():
    logging.info("Connecting to Redshift...")

    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DB,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    truncate_sql = "TRUNCATE TABLE test;"
    insert_sql = """
        INSERT INTO test
        SELECT * FROM uainventory.leads_lead
        WHERE date(created_at + interval '330' minute) = date(current_timestamp + interval '330' minute);
    """

    with conn.cursor() as cur:
        logging.info("Executing TRUNCATE...")
        cur.execute(truncate_sql)

        logging.info("Executing INSERT...")
        cur.execute(insert_sql)

    conn.commit()
    conn.close()
    logging.info("ETL completed successfully.")

