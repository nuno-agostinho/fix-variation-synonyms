#!/usr/bin/env python3

import argparse
import json
import mysql.connector
import warnings

## Read command-line arguments
parser = argparse.ArgumentParser(
  description='Fix variation_synonyms based on dbSNP refsnp-merged.json')
parser.add_argument('--json', help="JSON file", required=True)
parser.add_argument('--host', help="Database host", required=True)
parser.add_argument('--port', help="Database port", required=True)
parser.add_argument('--user', help="Database user", required=True)
parser.add_argument('--password', '--pass', help="Database password", default=None)
parser.add_argument('--database', help="Database name", required=True)
parser.add_argument('--log', help="Output log file", default="changed_data.out")
parser.add_argument('--sql_updates', help="File to output SQL update statements", default="dbSNP_synonyms_updates.sql")
parser.add_argument('--sql_inserts', help="File to output SQL insert statements", default="dbSNP_synonyms_inserts.sql")
args = parser.parse_args()

## Database functions

def connect_db(args):
  db = mysql.connector.connect(
    host = args.host,
    port = args.port,
    user = args.user,
    password = args.password,
    database = args.database)
  return db

def get_variation_id(cursor, rsid, table="variation"):
  '''
  Get variation_id from variation table
  '''
  sql = f"SELECT * FROM {table} WHERE name = 'rs{rsid}'"
  result = cursor.execute(sql)
  rows = cursor.fetchall()

  variation_id = None
  if len(rows) == 1:
    # get variation_id for the variant
    variation_id = rows[0][0]
  elif len(rows) > 1:
    warnings.warn(f"WARNING: more than one result for rs{rsid} in '{table}' table")
  else:
    warnings.warn(f"WARNING: no result for rs{rsid} in '{table}' table")

  return variation_id

def update_variation_id_synonyms(db, cursor, synonyms_rsid, variation_id, table="variation_synonym"):
  '''
  Update variation_id in variation_synonyms table for a list of given names (rsID)
  '''
  synonyms_str = ", ".join(["'rs" + x + "'" for x in synonyms_rsid])

  ## Change database
  sql = f"SELECT name, variation_id FROM {table} WHERE name IN ({synonyms_str});"
  result = cursor.execute(sql)
  rows = cursor.fetchall()

  changed_rows = []
  sql_updates  = []
  sql_inserts  = []
  if len(rows) > 0:
    for row in rows:
      if row[1] != variation_id:
        changed_rows = changed_rows + [row]

    # Update row if any variation_id is incorrect
    if len(changed_rows) > 0:
      sql = f"UPDATE IGNORE {table} SET variation_id = {variation_id} WHERE name IN ({synonyms_str});"
      sql_updates.append(sql)
      #cursor.execute(sql)
      #db.commit()
  else:
    # No variants found: insert values
    for syn in synonyms_rsid:
      sql  = f"INSERT INTO {table} (variation_id, source_id, name) VALUES ({variation_id}, '2', 'rs{syn}');"
      sql_inserts.append(sql)
      #cursor.execute(sql)
      #db.commit()
      changed_rows = changed_rows + [('rs' + syn, "NA")]

  return [changed_rows, sql_updates, sql_inserts]

def write_list_to_file(log, list, mode="a"):
  f = open(log, mode)
  for text in list:
    f.write(str(text) + "\n")
  f.close()

def write_rows_to_file(log, variation_id, rows, mode="a"):
  f = open(log, mode)
  for row in rows:
    f.write(str(row[0]) + "\t" + str(row[1]) + "\t" + str(variation_id) + "\n")
  f.close()

## Read JSON file and update database
if __name__ == "__main__":
  db = connect_db(args)
  f = open(args.json)
  count = 0

  # Clear log/output files
  #log = open(args.log, "w")
  #log.write("#rsID\tReturned\tdbSNP variation_id\n")
  #log.close()

  #log = open(args.sql_updates, "w")
  #log.write("# SQL update statements to fix dbSNP synonyms\n")
  #log.close()

  #log = open(args.sql_inserts, "w")
  #log.write("# SQL insert statements to fix dbSNP synonyms\n")
  #log.close()

  for line in f:
    data = json.loads(line)

    refsnp_id   = data["refsnp_id"]
    merged_rsid = [x["merged_rsid"] for x in data["dbsnp1_merges"]]
    merged_into = data["merged_snapshot_data"]["merged_into"]

    if len(merged_into) == 1:
      merged_into = merged_into[0]
    else:
     warnings.warn(f"WARNING: skipping rs{refsnp_id}: dbSNP merged it into two or more rsIDs")
     continue

    cursor = db.cursor(buffered=True)
    merged_into_variation_id = get_variation_id(cursor, merged_into)
    if merged_into_variation_id is None:
      warnings.warn(f"WARNING: skipping rs{refsnp_id}")
      continue

    merged_rsid = [refsnp_id] + merged_rsid
    [changed_rows, sql_updates, sql_inserts] = update_variation_id_synonyms(db, cursor, merged_rsid, merged_into_variation_id)
    if changed_rows is not None:
      write_rows_to_file(args.log, merged_into_variation_id, changed_rows)
    if sql_updates is not None:
      write_list_to_file(args.sql_updates, sql_updates)
    if sql_inserts is not None:
      write_list_to_file(args.sql_inserts, sql_inserts)

  f.close()
  db.close()
