#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 13:50:01 2025

@author: rikcrijns
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 10 11:36:38 2025

@author: rikcrijns
"""

import math
from typing import List
import chardet
import pandas as pd
from mysql.connector import connect, MySQLConnection
from datetime import datetime
import json
import logging
from google.cloud import secretmanager
import io
from paramiko import Transport, SFTPClient, SFTPAttributes
import re
import requests
import sys
import traceback
from google.cloud import logging as cloud_logging
from flask import Flask
import os

app = Flask(__name__)


def setup_google_cloud_logging():
    # 1) grab the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 2) remove any handlers it already has (so we don't double‑log)
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    # 3) add the Cloud Logging handler
    client = cloud_logging.Client()
    client.setup_logging()   # wires itself into the root logger

    # 4) also add a console handler if you still want stdout
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    return root_logger

# initialize once
logger = setup_google_cloud_logging()


# Correcting the exception handling to log with severity
def log_uncaught_exceptions(ex_cls, ex, tb):
    logging.error(''.join(traceback.format_tb(tb)))
    logging.error(f'{ex_cls.__name__}: {str(ex)}')

sys.excepthook = log_uncaught_exceptions# API Base URL


def access_secret_version(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/637358609369/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload


# Fetch credentials from Secret Manager
credentials_telforce_api = json.loads(access_secret_version("telforce_api_credentials"))
credentials_merkle_sftp = json.loads(access_secret_version("Merkle_SFTP_credentials"))

api_url = 'http://fonky.telforce.nl/vicidial/non_agent_api.php'

# Common API parameters
api_params_common = {
    'source': 'google_cloud',
    'user': credentials_telforce_api['user'],
    'pass': credentials_telforce_api['pass']
}


def update_status_lead(lead_id, status):
    """Function to update the status of a lead based on the phone number."""
    api_params = {**api_params_common, 'function': 'update_lead','search_method':'LEAD_ID','lead_id':lead_id,'status':status, 'search_location': 'LIST'}
    response = requests.get(api_url, params=api_params)
    print(f"Updating lead with lead id {lead_id} to {status}  {response.text} Status code: {response.status_code}")
    if response.ok:
        print(f"Lead {lead_id} updated to  successfully.")
        return {'success': True, 'lead_id': lead_id, 'message': 'Lead updated successfully'}
    print(f"Failed to delete lead {lead_id}: {response.text}")
    return {'success': False, 'lead_id': lead_id, 'error': response.text}

file_name_export_types = {
    'T_61_Ontdubbelingen_100000': 'NBB',
    'T_61_Ontdubbelingen_000000': 'MA',
    '_CHECK_OPTOUT_61': 'OPTOUT'
}

MA = [204,215,217]
NBB = [202,205,209,210,211,218]
OPTOUT = [204,215,217,202,205,209,210,211,218]



def expand_variants(numbers: List[str]) -> List[str]:
    """Return [num, '0'+num, '31'+num] for each num, then de-dupe."""
    variants = []
    for n in numbers:
        variants.extend((n, f"0{n}", f"31{n}"))
    return list(dict.fromkeys(variants))       # preserves order, removes dupes


# ────────────────────────────────────────────────────────────────────
# 2.  Query MySQL in batches (cursor opened/closed manually)
# ────────────────────────────────────────────────────────────────────
def fetch_vicidial_batches(
    numbers: List[str],
    list_ids: List[int],                # ← NEW: pass MA or NBB here
    batch_size: int = 3000
) -> pd.DataFrame:
    """
    Query vicidial_list in batches.

    Parameters
    ----------
    numbers    : list[str]   – phone-number variants to look up
    list_ids   : list[int]   – allowed v.list_id values (MA or NBB set)
    batch_size : int         – max IN-clause size per round-trip

    Returns
    -------
    DataFrame with the matching rows.
    """
    if not numbers:
        raise ValueError("Number list is empty after cleaning.")
    if not list_ids:
        raise ValueError("list_ids may not be empty.")

    conn: MySQLConnection = connect(
        host="fonky1.telforce.nl",
        database="asterisk",
        user="fonky",
        password="UQWKV@xHf&2A",
    )

    # SQL with two placeholder blocks: {ph} for phone numbers, {li} for list_ids
    sql_tpl = """
        SELECT DISTINCT
            v.lead_id              AS lead_id,
            v.phone_number         AS telefoonnummer,
            v.status               AS subresponsecode,
            v.list_id              AS list_id,
            v.entry_date           AS entry_date,
            v.last_local_call_time AS last_local_call_time
        FROM vicidial_list v
        LEFT JOIN vicidial_agent_log al ON v.lead_id = al.lead_id
        WHERE v.phone_number IN ({ph})
          AND YEAR(v.entry_date)  >= 2025
          AND DATE(v.entry_date)  >= CURDATE() - INTERVAL 30 DAY
          AND v.list_id IN ({li})
          AND v.status NOT IN (
                '4031','4021','1010','1020','4035','40101','1017','4022',
                '40112','ONTDUB','DNCL','40129','1012','40122','4010','1007','40115','40128','4011','4061',
                '1006','4033','40123','40134','4041', 'INCALL','4042'
                
                
          )
    """

    # Prepare the constant part for list_ids once
    li_placeholders = ", ".join(["%s"] * len(list_ids))

    all_rows: list[dict] = []
    total_batches = math.ceil(len(numbers) / batch_size)

    cur = conn.cursor(dictionary=True)
    try:
        for i in range(total_batches):
            batch = numbers[i * batch_size : (i + 1) * batch_size]

            ph_placeholders = ", ".join(["%s"] * len(batch))
            sql = sql_tpl.format(ph=ph_placeholders, li=li_placeholders)

            # Order of params == order of placeholders
            params = batch + list_ids
            cur.execute(sql, params)

            rows = cur.fetchall()
            all_rows.extend(rows)

            print(
                f"Batch {i + 1}/{total_batches}  "
                f"→ {len(batch):,} nums  → {len(rows):,} rows"
            )
    finally:
        cur.close()
        conn.close()

    return pd.DataFrame(all_rows)


def detect_encoding(data: bytes) -> str:
    """
    Detect the encoding of a byte string.

    Parameters
    ----------
    data : bytes
        Raw file contents.

    Returns
    -------
    str
        The most probable encoding (falls back to 'utf-8' if chardet is unsure).
    """
    result = chardet.detect(data)
    return result["encoding"] or "utf-8"

def get_sftp_files() -> list[pd.DataFrame]:
    """
    Download exactly one (the newest) file for every export code in
    `file_name_export_types` and return a list of DataFrames.
    """
    # --- 1. Connect ---------------------------------------------------------
    SFTP_HOST = credentials_merkle_sftp["sftp_host"]
    SFTP_PORT = int(credentials_merkle_sftp["sftp_port"])
    SFTP_USERNAME = credentials_merkle_sftp["sftp_username"]
    SFTP_PASSWORD = credentials_merkle_sftp["sftp_password"]
    SFTP_DIRECTORY = credentials_merkle_sftp["sftp_directory"]

    regex_by_code = {
        code: re.compile(fr"^XMP{re.escape(code)}.*\.txt$", re.I)
        for code in file_name_export_types.keys()
    }

    df_list: list[pd.DataFrame] = []
    newest_attr: dict[str, SFTPAttributes] = {}

    with Transport((SFTP_HOST, SFTP_PORT)) as transport:
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        with SFTPClient.from_transport(transport) as sftp:

            # --- 2. Find the newest file per code ---------------------------
            for attr in sftp.listdir_attr(SFTP_DIRECTORY):
                fname = attr.filename
                for code, regex in regex_by_code.items():
                    if regex.match(fname):
                        # keep the file with the largest st_mtime
                        if (code not in newest_attr or
                                attr.st_mtime > newest_attr[code].st_mtime):
                            newest_attr[code] = attr
                        break   # a file can match only one code

            if len(newest_attr) < len(regex_by_code):
                missing = set(regex_by_code) - set(newest_attr)
                logging.warning(
                    "No recent file found for codes: %s", ", ".join(missing)
                )

            # --- 3. Download exactly those files -----------------------------
            for code, attr in newest_attr.items():
                full_path = f"{SFTP_DIRECTORY}/{attr.filename}"
                logging.info("Downloading %s", attr.filename)
                with sftp.open(full_path, "rb") as fh:
                    raw = fh.read()

                enc = detect_encoding(raw)
                df = pd.read_csv(
                        io.BytesIO(raw),
                        sep="|",
                        dtype=str,
                        encoding=enc,
                        keep_default_na=False,   # ← NEW
                        na_values=[]             # ← NEW (no strings treated as NA)
                )
                df["export_type"] = file_name_export_types[code]
                df["source_file"] = attr.filename
                df_list.append(df)

    return df_list

def strip_first_zero(num) -> str:
    if pd.isna(num):
        return ""
    num = str(num).strip()
    return num[1:] if num.startswith("0") else num
# ────────────────────────────────────────────────────────────────────
# 3.  Glue code  –  headless + structured logging
# ────────────────────────────────────────────────────────────────────
def main():
    run_start = datetime.now()
    timestamp  = run_start.strftime("%Y%m%d_%H%M%S")
    logging.info("==== Ontdubbel run started: %s ====", run_start.isoformat(timespec="seconds"))

    # 0. fetch the newest files (may warn if one is missing)
    sftp_dfs = get_sftp_files()
    if not sftp_dfs:
        logging.error("No files retrieved from SFTP – aborting.")
        raise SystemExit(1)

    for df_file in sftp_dfs:
        step_start = datetime.now()

        export_type = df_file["export_type"].iloc[0]   # NBB / MA / OPTOUT
        src_fname   = df_file["source_file"].iloc[0]
        logging.info("[%s] ── Processing %s", export_type, src_fname)

        # ─────────────────────────────────────────────────────────
        # 1.  extract + clean phone numbers
        # ─────────────────────────────────────────────────────────
      # ─────────────────────────────────────────────────────────
        # 1.  extract + clean phone numbers
        # ─────────────────────────────────────────────────────────
        if export_type in ("NBB", "MA"):
            cols_to_use = []
            if "CONTACT_TELEFOONNUMMER_3" in df_file.columns:
                cols_to_use.append(df_file["CONTACT_TELEFOONNUMMER_3"])
            if "CONTACT_TELEFOONNUMMER_1" in df_file.columns:
                cols_to_use.append(df_file["CONTACT_TELEFOONNUMMER_1"])

            if len(cols_to_use) > 1:
                merged = pd.concat(cols_to_use, ignore_index=True)
            elif len(cols_to_use) == 1:
                merged = cols_to_use[0]
            else:
                logging.warning("[%s] No telefoonnummer columns found – skipping", export_type)
                continue
        else:  # OPTOUT
            if "PHONE_NUMBER" in df_file.columns:
                merged = df_file["PHONE_NUMBER"]
            else:
                logging.warning("[%s] No PHONE_NUMBER column found – skipping", export_type)
                continue

        cleaned_numbers = (
            merged.map(strip_first_zero)
                  .loc[lambda s: s.ne("")]
                  .drop_duplicates()
                  .tolist()
        )
        
        
        logging.info("[%s] Cleaned %s unique numbers",
                     export_type, f"{len(cleaned_numbers):,}")

        # ─────────────────────────────────────────────────────────
        # 2. query Vicidial
        # ─────────────────────────────────────────────────────────
        list_ids = {"MA": MA, "OPTOUT": OPTOUT}.get(export_type, NBB)
        df_result = fetch_vicidial_batches(cleaned_numbers, list_ids)
        logging.info("[%s] Vicidial query → %s rows",
                     export_type, f"{len(df_result):,}")

        # ─────────────────────────────────────────────────────────
        # 3. save query result
        # ─────────────────────────────────────────────────────────
        out_dir  = r"/Users/rikcrijns/Documents/Python Script/Ontdubbel"
        out_file = (
            f"{out_dir}/vicidial_query_result_ontdubbel_"
            f"{export_type}_{timestamp}.csv"
        )
        df_result.to_csv(out_file, index=False)
        logging.info("[%s] Saved result to %s", export_type, out_file)

        # ─────────────────────────────────────────────────────────
        # 4. console report (still handy when running interactively)
        # ─────────────────────────────────────────────────────────
        
        if not df_result.empty:
            logging.info("[%s] Status counts:\n%s",
                          export_type,
                          df_result["subresponsecode"]
                          .fillna("(empty)")
                          .value_counts()
                          .to_string())
    
            logging.info("[%s] List-ID counts:\n%s",
                          export_type,
                          df_result["list_id"]
                          .fillna("(empty)")
                          .value_counts()
                          .to_string())
            # ─────────────────────────────────────────────────────────
            # 5. update lead status via API
            # ─────────────────────────────────────────────────────────
            new_status = "ONTDUB" if export_type in ("NBB", "MA") else "DNCL"
            successes, failures = 0, 0
            for lead_id in df_result["lead_id"]:
                resp = update_status_lead(lead_id, new_status)
                successes += resp["success"]
                failures  += not resp["success"]

            duration = (datetime.now() - step_start).total_seconds()
            logging.info("[%s] Done   (%.1f s)  →  %s updates OK, %s failed",
                         export_type, duration, successes, failures)
        else: 
            logging.info("No matching leads in the database")
            
                



    total_time = (datetime.now() - run_start).total_seconds()
    logging.info("==== Ontdubbel run finished in %.1f seconds ====", total_time)

@app.route('/')
def run_main():
    logging.info("Received request at '/' endpoint")
    try:
        main()
    except Exception as e:
        logging.error("Error occurred", exc_info=True)
        return "Internal Server Error", 500
    return "Script executed successfully."

if __name__ == '__main__':
    logging.info("Starting Flask application.")
    port = int(os.environ.get('PORT', 8080))
    app.debug = True
    app.run(host='0.0.0.0', port=port)
        


