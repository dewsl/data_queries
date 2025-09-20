# -*- coding: utf-8 -*-
"""
Created on Sat Sep 20 11:00:44 

Script for adding a new site entry into commons_db.sites
Created separately to avoid overloading logger update script

@author: nichm
"""

import configparser
import mysql.connector
from mysql.connector import Error
import os

def get_db_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "config.cnf")

    config = configparser.ConfigParser()
    config.read(config_file)
    db_config = {
        "host": config["db"]["host"],
        "database": config["db"]["database"],
        "user": config["db"]["user"],
        "password": config["db"]["password"],
    }
    return db_config

def prompt_with_skip(message):
    val = input(message).strip()
    return val if val != "" else None

def add_new_site(connection):
    cursor = connection.cursor()

    # sanity check
    confirm = input("Are you adding a new site? (1 for Yes, 0 for No): ").strip()
    if confirm != "1":
        print("Exiting. No new site created.")
        return

    print("\n--- Enter Site Details ---")
    site_code = input("Enter site code (e.g. abc): ").strip()
    purok = prompt_with_skip("Enter purok (press Enter to skip): ")
    sitio = prompt_with_skip("Enter sitio (press Enter to skip): ")
    barangay = input("Enter barangay: ").strip()
    municipality = input("Enter municipality: ").strip()
    province = input("Enter province: ").strip()
    region = input("Enter region: ").strip()
    psgc = prompt_with_skip("Enter PSGC (press Enter to skip): ")
    households = prompt_with_skip("Enter number of households (press Enter to skip): ")
    season = prompt_with_skip("Enter season info (press Enter to skip): ")
    area_code = prompt_with_skip("Enter area code (press Enter to skip): ")
    latitude = float(input("Enter latitude (decimal degrees): ").strip())
    longitude = float(input("Enter longitude (decimal degrees): ").strip())

    # Auto values
    active = 1
    has_cbewsl = 0
    is_mlgu_handled = 0
    has_groupchat = 0
    messenger_group_chat_link = None

    # Get next site_id (increment)
    cursor.execute("SELECT COALESCE(MAX(site_id), 0) + 1 FROM commons_db.sites")
    site_id = cursor.fetchone()[0]

    insert_query = """
        INSERT INTO commons_db.sites 
        (site_id, site_code, purok, sitio, barangay, municipality, province, region, psgc,
         active, households, season, area_code, latitude, longitude,
         has_cbewsl, is_mlgu_handled, has_groupchat, messenger_group_chat_link)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (site_id, site_code, purok, sitio, barangay, municipality,
                                  province, region, psgc, active, households, season,
                                  area_code, latitude, longitude,
                                  has_cbewsl, is_mlgu_handled, has_groupchat,
                                  messenger_group_chat_link))
    connection.commit()
    print(f"✅ New site {site_code} inserted successfully with site_id {site_id}.")

def main():
    try:
        db_config = get_db_config()
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            print("Connected to database.")
            add_new_site(connection)

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    main()
