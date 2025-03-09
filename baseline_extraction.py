import csv
import os
import sqlite3


def create_connection(db_file):
    """Creates a read-only database connection"""
    if not os.path.exists(db_file):
        print(f"❌ Error: Database file '{db_file}' does not exist.")
        return None
    
    try:
        conn = sqlite3.connect(f"file:{db_file}", uri=True, check_same_thread=False)
        return conn
    except sqlite3.Error as e:
        print(f"❌ Database connection error: {e}")
        return None

def createIndexies(db_file):
    conn = create_connection(db_file)
    cur = conn.cursor()

    INDEX_COLUMNS = {
    "cYear": ["countryCode", "euRBDCode"],  # Common composite index
    "euSurfaceWaterBodyCode": ["cYear", "countryCode", "euRBDCode"],  # SWB Tables
    "euGroundWaterBodyCode": ["cYear", "countryCode", "euRBDCode"],  # GWB Tables
    }
    # Fetch all table names
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cur.fetchall()]

    # Loop through each table and extract columns
    for table in tables:
    # Get existing indexes for the table
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{table}';")
        existing_indexes = {row[0] for row in cur.fetchall()}

        # Fetch columns for the current table
        cur.execute(f"PRAGMA table_info({table});")
        columns = [row[1] for row in cur.fetchall()]

        for base_col, related_cols in INDEX_COLUMNS.items():
            if base_col in columns:
                index_name = f"idx_{table}_{base_col}"
                index_columns = [base_col] + [col for col in related_cols if col in columns]
                index_columns_str = ", ".join(index_columns)

                if index_name in existing_indexes:
                    print(f"Skipping existing index: {index_name}")
                    continue

                sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({index_columns_str});"
                cur.execute(sql)
                print(f"Created index on {table}: {index_columns_str}")

    # Commit and close
    conn.commit()
    print("Index process completed")

def updateTables(db_file):
    """
    Cleans up whitespace (spaces, tabs, newlines) from C_StatusFailing and C_StatusKnown columns
    in the swRBD_Europe_data table.
    """

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    try:
        cur = conn.cursor()

        # 🚀 **Optimized SQL UPDATE Queries**
        queries = [
            "UPDATE swRBD_Europe_data SET C_StatusFailing = TRIM(REPLACE(REPLACE(REPLACE(C_StatusFailing, ' ', ''), '\t', ''), '\n', ''));",
            "UPDATE swRBD_Europe_data SET C_StatusKnown = TRIM(REPLACE(REPLACE(REPLACE(C_StatusKnown, ' ', ''), '\t', ''), '\n', ''));"
        ]

        for query in queries:
            cur.execute(query)

        conn.commit()
        print("✅ Tables updated successfully!")

    except sqlite3.Error as e:
        print(f"❌ Error updating tables: {e}")
        conn.rollback()  # Rollback in case of error

    finally:
        conn.close()

def create_and_populate_swRBD_Europe_data(db_file):
    """
    Creates the swRBD_Europe_data table and populates it with predefined data.
    """

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    # **Drop the table if it exists**
    cur.execute("DROP TABLE IF EXISTS swRBD_Europe_data")

    # **Create the table with all columns**
    create_table_query = """
    CREATE TABLE swRBD_Europe_data (
        euRBDCode_ TEXT,
        C_StatusFailing INTEGER,
        C_StatusFailingPercent TEXT,
        C_StatusKnown INTEGER,
        C_Unit TEXT,
        countryName TEXT,
        euRBDCode TEXT,
        NUTS0 TEXT,
        rbdName TEXT,
        geography TEXT,
        Latitude REAL,
        Longitude REAL,
        P_StatusFailing TEXT
    );
    """
    cur.execute(create_table_query)

    # **Predefined Data**
    data = [
        ("UKGI17", 0, None, 0, "water bodies", "United Kingdom", "UKGI17", "UK", "GIBRALTAR", "Polygon", 36.132335586, -5.352301173, "Surface water status"),
        ("UKGBNINE", 83, "85%", 98, "water bodies", "United Kingdom", "UKGBNINE", "UK", "NORTH EASTERN", "MultiPolygon", 54.737684353, -5.994902187, "Surface water status"),
        ("UKGBNIIENW", 94, "69%", 137, "water bodies", "United Kingdom", "UKGBNIIENW", "UK", "NORTH WESTERN", "MultiPolygon", 54.589725180, -7.384038916, "Surface water status"),
        ("UKGBNIIENB", 153, "88%", 173, "water bodies", "United Kingdom", "UKGBNIIENB", "UK", "NEAGH BANN", "MultiPolygon", 54.579872738, -6.538658676, "Surface water status"),
        ("UK12", 481, "78%", 613, "water bodies", "United Kingdom", "UK12", "UK", "NORTH WEST", "Polygon", 53.909889033, -2.767159292, "Surface water status"),
        ("UK11", 69, "74%", 93, "water bodies", "United Kingdom", "UK11", "UK", "DEE", "Polygon", 53.051375231, -3.257519396, "Surface water status"),
        ("UK10", 320, "59%", 543, "water bodies", "United Kingdom", "UK10", "UK", "WESTERN WALES", "MultiPolygon", 52.381929105, -4.115345073, "Surface water status"),
        ("UK09", 605, "80%", 755, "water bodies", "United Kingdom", "UK09", "UK", "SEVERN", "Polygon", 52.130751392, -2.514628733, "Surface water status"),
        ("UK08", 535, "77%", 697, "water bodies", "United Kingdom", "UK08", "UK", "SOUTH WEST", "MultiPolygon", 50.711361898, -3.763656524, "Surface water status"),
        ("UK07", 239, "85%", 282, "water bodies", "United Kingdom", "UK07", "UK", "SOUTH EAST", "Polygon", 51.053060259, -0.155757235, "Surface water status"),
        ("UK06", 459, "92%", 498, "water bodies", "United Kingdom", "UK06", "UK", "THAMES", "Polygon", 51.555803631, -0.601855894, "Surface water status"),
        ("UK05", 538, "89%", 603, "water bodies", "United Kingdom", "UK05", "UK", "ANGLIAN", "Polygon", 52.431680924, 0.179476345, "Surface water status"),
        ("UK04", 840, "85%", 987, "water bodies", "United Kingdom", "UK04", "UK", "HUMBER", "Polygon", 53.494128407, -1.264894166, "Surface water status"),
        ("UK03", 277, "74%", 374, "water bodies", "United Kingdom", "UK03", "UK", "NORTHUMBRIA", "Polygon", 54.996797875, -1.839283259, "Surface water status"),
        ("UK02", 315, "56%", 559, "water bodies", "United Kingdom", "UK02", "UK", "SOLWAY TWEED", "MultiPolygon", 55.147044013, -3.292553564, "Surface water status"),
        ("UK01", 1189, "42%", 2825, "water bodies", "United Kingdom", "UK01", "UK", "SCOTLAND", "MultiPolygon", 57.829491799, -4.146001326, "Surface water status"),
        ("SK40000", 652, "45%", 1436, "water bodies", "Slovakia", "SK40000", "SK", "DANUBE", "Polygon", 48.734960454, 19.703706324, "Surface water status"),
        ("SK30000", 19, "26%", 74, "water bodies", "Slovakia", "SK30000", "SK", "VISTULA", "Polygon", 49.228712120, 20.478574331, "Surface water status"),
        ("SIRBD2", 33, "100%", 33, "water bodies", "Slovenia", "SIRBD2", "SI", "ADRIATIC", "Polygon", 45.883634061, 13.860645946, "Surface water status"),
        ("SIRBD1", 121, "100%", 121, "water bodies", "Slovenia", "SIRBD1", "SI", "DANUBE", "Polygon", 46.192931659, 15.107100733, "Surface water status"),
        ("SE5101", 55, "100%", 55, "water bodies", "Sweden", "SE5101", "SE", "SKAGERRAK AND KATTEGAT (INTERNATIONAL DRAINAGE BASIN GLOMMA - SWEDEN)", "MultiPolygon", 60.257236405, 12.013145837, "Surface water status"),
        ("SE1104", 4, "100%", 4, "water bodies", "Sweden", "SE1104", "SE", "BOTHNIAN BAY (INTERNATIONAL DRAINAGE BASIN TROMS - SWEDEN)", "MultiPolygon", 68.615772967, 19.781902521, "Surface water status"),
        ("SE1103", 116, "100%", 116, "water bodies", "Sweden", "SE1103", "SE", "BOTHNIAN BAY (INTERNATIONAL DRAINAGE BASIN NORDLAND - SWEDEN)", "MultiPolygon", 66.459903101, 15.691246333, "Surface water status"),
        ("SE1102", 65, "100%", 65, "water bodies", "Sweden", "SE1102", "SE", "BOTHNIAN SEA (INTERNATIONAL DRAINAGE BASIN TRONDELAGSFYLKENE - SWEDEN)", "MultiPolygon", 63.354535687, 12.321826378, "Surface water status"),
        ("SE5", 2591, "100%", 2591, "water bodies", "Sweden", "SE5", "SE", "SKAGERRAK AND KATTEGAT (SWEDEN)", "MultiPolygon", 58.780320556, 13.091225953, "Surface water status"),
        ("SE4", 1714, "100%", 1714, "water bodies", "Sweden", "SE4", "SE", "SOUTH BALTIC SEA (SWEDEN)", "MultiPolygon", 57.187615846, 15.450830435, "Surface water status"),
        ("SE3", 1218, "100%", 1218, "water bodies", "Sweden", "SE3", "SE", "NORTH BALTIC SEA (SWEDEN)", "Polygon", 59.592241150, 16.575895606, "Surface water status"),
        ("SE2", 10593, "100%", 10593, "water bodies", "Sweden", "SE2", "SE", "BOTHNIAN SEA (SWEDEN)", "MultiPolygon", 62.602767843, 15.338659807, "Surface water status"),
        ("SE1TO", 940, "100%", 940, "water bodies", "Sweden", "SE1TO", "SE", "BOTHNIAN BAY (INTERNATIONAL DISTRICT TORNE RIVER - SWEDEN)", "Polygon", 67.588752779, 21.835904437, "Surface water status"),
        ("SE1", 5889, "100%", 5889, "water bodies", "Sweden", "SE1", "SE", "BOTHNIAN BAY (SWEDEN)", "MultiPolygon", 66.000874088, 19.226189842, "Surface water status"),
        ("RO1000", 1050, "35%", 3021, "water bodies", "Romania", "RO1000", "RO", "DANUBE RIVER BASIN DISTRICT", "MultiPolygon", 45.815974224, 25.069743732, "Surface water status"),
        ("PTRH8", 22, "37%", 60, "water bodies", "Portugal", "PTRH8", "PT", "ALGARVE RIVERS", "Polygon", 37.197804786, -8.315887485, "Surface water status"),
        ("PTRH7", 158, "95%", 167, "water bodies", "Portugal", "PTRH7", "PT", "GUADIANA", "Polygon", 38.245608733, -7.513078791, "Surface water status"),
        ("PTRH6", 135, "90%", 150, "water bodies", "Portugal", "PTRH6", "PT", "SADO AND MIRA", "Polygon", 38.046672950, -8.437790422, "Surface water status"),
        ("PTRH5A", 238, "95%", 250, "water bodies", "Portugal", "PTRH5A", "PT", "TAGUS AND WEST RIVERS", "MultiPolygon", 39.459857066, -8.183471774, "Surface water status"),
        ("PTRH4A", 70, "38%", 183, "water bodies", "Portugal", "PTRH4A", "PT", "VOUGA, MONDEGO AND LIS", "Polygon", 40.313207491, -8.316823311, "Surface water status"),
        ("PTRH3", 143, "84%", 171, "water bodies", "Portugal", "PTRH3", "PT", "DOURO", "Polygon", 41.205379577, -7.391668390, "Surface water status"),
        ("PTRH2", 37, "82%", 45, "water bodies", "Portugal", "PTRH2", "PT", "CAVADO, AVE AND LECA", "Polygon", 41.542519063, -8.313033137, "Surface water status"),
        ("PTRH1", 23, "62%", 37, "water bodies", "Portugal", "PTRH1", "PT", "MINHO AND LIMA", "MultiPolygon", 41.873864339, -8.408900256, "Surface water status"),
        ("PL9000", 3, "100%", 3, "water bodies", "Poland", "PL9000", "PL", "DNIESTER RIVER BASIN DISTRICT", "Polygon", 49.395648129, 22.657851790, "Surface water status"),
        ("PL8000", 45, "68%", 66, "water bodies", "Poland", "PL8000", "PL", "NEMUNAS RIVER BASIN DISTRICT", "MultiPolygon", 53.726365244, 23.429637008, "Surface water status"),
        ("PL7000", 102, "52%", 195, "water bodies", "Poland", "PL7000", "PL", "PREGOLYA RIVER BASIN DISTRICT", "Polygon", 53.990502080, 21.278627599, "Surface water status"),
        ("PL6000", 1674, "81%", 2076, "water bodies", "Poland", "PL6000", "PL", "ODER RIVER BASIN DISTRICT", "MultiPolygon", 51.877398070, 17.141357783, "Surface water status"),
        ("PL5000", 3, "38%", 8, "water bodies", "Poland", "PL5000", "PL", "RIVER BASIN DISTRICT", "MultiPolygon", 50.469370017, 16.171936541, "Surface water status"),
        ("PL4000", 6, "100%", 6, "water bodies", "Poland", "PL4000", "PL", "JARFT RIVER BASIN DISTRICT", "Polygon", 54.360076522, 20.063235174, "Surface water status"),
        ("PL3000", 2, "40%", 5, "water bodies", "Poland", "PL3000", "PL", "SWIEZA RIVER BASIN DISTRICT", "MultiPolygon", 54.356309082, 20.505450893, "Surface water status"),
        ("PL2000", 2304, "77%", 3006, "water bodies", "Poland", "PL2000", "PL", "VISTULA RIVER BASIN DISTRICT", "MultiPolygon", 52.157844631, 20.589784082, "Surface water status"),
        ("PL1000", 6, "55%", 11, "water bodies", "Poland", "PL1000", "PL", "DANUBE RIVER BASIN DISTRICT", "MultiPolygon", 49.525503819, 19.473567202, "Surface water status"),
        ("NOVHA6", 0, "", 0, "water bodies", "Norway", "NOVHA6", "NO", "TORNIONJOKI", "MultiPolygon", 69.029919089, 21.883108243, "Surface water status"),
        ("NOVHA5", 0, "", 0, "water bodies", "Norway", "NOVHA5", "NO", "KEMIJOKI", "MultiPolygon", 68.748348549, 24.111951806, "Surface water status"),
        ("NO5106", 1139, "99%", 1150, "water bodies", "Norway", "NO5106", "NO", "SOGN OG FJORDANE", "Polygon", 61.383353268, 6.540991353, "Surface water status"),
        ("NO5105", 1000, "99%", 1007, "water bodies", "Norway", "NO5105", "NO", "HORDALAND", "Polygon", 60.338820141, 6.098638009, "Surface water status"),
        ("NO5104", 626, "99%", 635, "water bodies", "Norway", "NO5104", "NO", "ROGALAND", "MultiPolygon", 59.222805919, 6.266138232, "Surface water status"),
        ("NO5103", 1545, "99%", 1565, "water bodies", "Norway", "NO5103", "NO", "AGDER", "Polygon", 58.840662251, 7.670264206, "Surface water status"),
        ("NO5102", 1318, "98%", 1346, "water bodies", "Norway", "NO5102", "NO", "WEST-BAY", "MultiPolygon", 60.103842078, 8.855591654, "Surface water status"),
        ("NO5101", 829, "97%", 852, "water bodies", "Norway", "NO5101", "NO", "GLOMMA", "MultiPolygon", 61.209243264, 10.501635759, "Surface water status"),
        ("NO1106", 117, "87%", 134, "water bodies", "Norway", "NO1106", "NO", "NORWEGIAN - FINNISH", "MultiPolygon", 69.783891433, 27.254560873, "Surface water status"),
        ("NO1105", 108, "70%", 154, "water bodies", "Norway", "NO1105", "NO", "FINNMARK", "MultiPolygon", 70.120102834, 25.468063645, "Surface water status"),
        ("NO1104", 223, "91%", 244, "water bodies", "Norway", "NO1104", "NO", "TROMS", "MultiPolygon", 69.277493257, 19.458353357, "Surface water status"),
        ("NO1103", 557, "98%", 569, "water bodies", "Norway", "NO1103", "NO", "NORDLAND", "MultiPolygon", 67.239429088, 14.609085373, "Surface water status"),
        ("NO1102", 1099, "99%", 1111, "water bodies", "Norway", "NO1102", "NO", "TROENDELAG", "MultiPolygon", 64.033271443, 11.374627680, "Surface water status"),
        ("NO1101", 390, "98%", 397, "water bodies", "Norway", "NO1101", "NO", "MOERE AND ROMSDAL", "Polygon", 62.617046899, 7.733528269, "Surface water status"),
        ("NO5", 87, "89%", 98, "water bodies", "Norway", "NO5", "NO", "SKAGERRAK AND KATTEGAT", "MultiPolygon", 61.012581708, 11.975848292, "Surface water status"),
        ("NO2", 21, "91%", 23, "water bodies", "Norway", "NO2", "NO", "BOTHNIAN SEA", "MultiPolygon", 63.619042063, 13.026928056, "Surface water status"),
        ("NO1TO", 0, "", 0, "water bodies", "Norway", "NO1TO", "NO", "BOTHNIAN BAY (INTERNATIONAL DISTRICT TORNE RIVER - NORWAY)", "MultiPolygon", 68.590197687, 19.138782762, "Surface water status"),
        ("NO1", 3, "100%", 3, "water bodies", "Norway", "NO1", "NO", "BOTHNIAN BAY", "MultiPolygon", 67.115797183, 15.988907849, "Surface water status"),
        ("NLSC", 54, "100%", 54, "water bodies", "Netherlands", "NLSC", "NL", "SCHELDE", "Polygon", 51.531502328, 3.756676959, "Surface water status"),
        ("NLRN", 476, "100%", 477, "water bodies", "Netherlands", "NLRN", "NL", "RIJN", "Polygon", 52.551259534, 5.639992492, "Surface water status"),
        ("NLMS", 157, "100%", 157, "water bodies", "Netherlands", "NLMS", "NL", "MAAS", "MultiPolygon", 51.469649927, 5.144110137, "Surface water status"),
        ("NLEM", 21, "100%", 21, "water bodies", "Netherlands", "NLEM", "NL", "EEMS", "MultiPolygon", 53.270551342, 6.691060188, "Surface water status"),
        ("MTMALTA", 9, "100%", 9, "water bodies", "Malta", "MTMALTA", "MT", "MALTA", "Polygon", 35.934675243, 14.375171923, "Surface water status"),
        ("LVVUBA", 79, "95%", 83, "water bodies", "Latvia", "LVVUBA", "LV", "VENTAS UPJU BASEINU APGABALS", "Polygon", 56.852600284, 22.245436951, "Surface water status"),
        ("LVLUBA", 43, "100%", 43, "water bodies", "Latvia", "LVLUBA", "LV", "LIELUPES UPJU BASEINU APGABALS", "MultiPolygon", 56.506813033, 24.382250319, "Surface water status"),
        ("LVGUBA", 56, "97%", 58, "water bodies", "Latvia", "LVGUBA", "LV", "GAUJAS UPJU BASEINU APGABALS", "Polygon", 57.475384694, 25.485409276, "Surface water status"),
        ("LVDUBA", 196, "98%", 199, "water bodies", "Latvia", "LVDUBA", "LV", "DAUGAVAS UPJU BASEINU APGABALS", "MultiPolygon", 56.689702759, 26.276593287, "Surface water status"),
        ("LU001", 3, "100%", 3, "water bodies", "Luxembourg", "LU001", "LU", "MEUSE", "MultiPolygon", 49.632070585, 5.903020587, "Surface water status"),
        ("LU000", 107, "100%", 107, "water bodies", "Luxembourg", "LU000", "LU", "RHINE", "Polygon", 49.784517200, 6.096688524, "Surface water status"),
        ("LT4500", 10, "20%", 50, "water bodies", "Lithuania", "LT4500", "LT", "DAUGUVA", "MultiPolygon", 55.493289486, 26.345767364, "Surface water status"),
        ("LT3400", 123, "84%", 147, "water bodies", "Lithuania", "LT3400", "LT", "LIELUPE", "MultiPolygon", 56.020635864, 24.304422990, "Surface water status"),
        ("LT2300", 52, "45%", 115, "water bodies", "Lithuania", "LT2300", "LT", "VENTA", "Polygon", 56.047082912, 22.266933703, "Surface water status"),
        ("LT1100", 385, "44%", 873, "water bodies", "Lithuania", "LT1100", "LT", "NEMUNAS", "MultiPolygon", 55.217257592, 23.808194196, "Surface water status"),
        ("ITH", 126, "89%", 141, "water bodies", "Italy", "ITH", "IT", "RBD SICILIA", "MultiPolygon", 37.676918767, 14.051997052, "Surface water status"),
        ("ITG", 212, "27%", 786, "water bodies", "Italy", "ITG", "IT", "RBD SARDEGNA", "MultiPolygon", 40.144740967, 8.897607641, "Surface water status"),
        ("ITF", 337, "71%", 474, "water bodies", "Italy", "ITF", "IT", "RBD APPENNINO MERIDIONALE", "MultiPolygon", 40.503554813, 15.646868960, "Surface water status"),
        ("ITE", 338, "62%", 548, "water bodies", "Italy", "ITE", "IT", "RBD APPENNINO CENTRALE", "MultiPolygon", 42.296505913, 12.948285827, "Surface water status"),
        ("ITD", 28, "54%", 52, "water bodies", "Italy", "ITD", "IT", "RBD BACINO PILOTA SERCHIO", "Polygon", 44.012017326, 10.462309128, "Surface water status"),
        ("ITC", 915, "67%", 1362, "water bodies", "Italy", "ITC", "IT", "RBD APPENNINO SETTENTRIONALE", "MultiPolygon", 43.623264609, 10.745975494, "Surface water status"),
        ("ITB", 1051, "52%", 2024, "water bodies", "Italy", "ITB", "IT", "RBD PADANO", "MultiPolygon", 45.276990545, 9.416401580, "Surface water status"),
        ("ITA", 567, "36%", 1555, "water bodies", "Italy", "ITA", "IT", "RBD ALPI ORIENTALI", "Polygon", 46.049653748, 12.021098590, "Surface water status"),
        ("IEROI", 1023, "90%", 1138, "water bodies", "Ireland", "IEROI", "IE", "REPUBLIC OF IRELAND", "MultiPolygon", 53.062075547, -8.388099370, "Surface water status"),
        ("IEGBNIIENW", 238, "96%", 249, "water bodies", "Ireland", "IEGBNIIENW", "IE", "NORTH WESTERN", "MultiPolygon", 54.520841163, -7.746653358, "Surface water status"),
        ("IEGBNIIENB", 51, "98%", 52, "water bodies", "Ireland", "IEGBNIIENB", "IE", "NEAGH BANN", "MultiPolygon", 54.056583678, -6.658003359, "Surface water status"),
        ("HU1000", 848, "95%", 891, "water bodies", "Hungary", "HU1000", "HU", "HUNGARIAN PART OF THE DANUBE RIVER BASIN DISTRICT", "MultiPolygon", 47.251924863, 19.293451919, "Surface water status"),
        ("HRJ", 228, "55%", 413, "water bodies", "Croatia", "HRJ", "HR", "ADRIATIC RIVER BASIN DISTRICT", "MultiPolygon", 43.901619794, 15.969830324, "Surface water status"),
        ("HRC", 703, "61%", 1159, "water bodies", "Croatia", "HRC", "HR", "DANUBE RIVER BASIN DISTRICT", "MultiPolygon", 45.402906063, 16.848351687, "Surface water status"),
        ("FRH", 1447, "87%", 1672, "water bodies", "France", "FRH", "FR", "LA SEINE ET LES COURS D'EAU CÔTIERS NORMANDS", "MultiPolygon", 48.786598004, 2.115986334, "Surface water status"),
        ("FRG", 1540, "87%", 1761, "water bodies", "France", "FRG", "FR", "LA LOIRE, LES COURS D'EAU CÔTIERS VENDÉENS ET BRETONS", "MultiPolygon", 46.957612609, 0.889830570, "Surface water status"),
        ("FRF", 1619, "67%", 2403, "water bodies", "France", "FRF", "FR", "LA GARONNE, L'ADOUR, LA DORDOGNE, LA CHARENTE ET LES COURS D'EAU CÔTIERS CHARENTAIS ET AQUITAINS", "MultiPolygon", 44.401741403, 1.046886893, "Surface water status"),
        ("FRE", 42, "18%", 234, "water bodies", "France", "FRE", "FR", "LES COURS D'EAU DE LA CORSE", "Polygon", 42.186256489, 9.114989751, "Surface water status"),
        ("FRD", 1347, "48%", 2786, "water bodies", "France", "FRD", "FR", "LE RHÔNE ET LES COURS D'EAU CÔTIERS MÉDITERRANÉENS", "MultiPolygon", 44.998573435, 4.958006580, "Surface water status"),
        ("FRC", 414, "95%", 434, "water bodies", "France", "FRC", "FR", "LE RHIN", "Polygon", 48.598980267, 6.753195518, "Surface water status"),
        ("FRB2", 12, "100%", 12, "water bodies", "France", "FRB2", "FR", "LA SAMBRE", "Polygon", 50.130112459, 3.932729866, "Surface water status"),
        ("FRB1", 115, "94%", 122, "water bodies", "France", "FRB1", "FR", "LA MEUSE", "MultiPolygon", 49.092510928, 5.333260584, "Surface water status"),
        ("FRA", 66, "97%", 68, "water bodies", "France", "FRA", "FR", "L'ESCAUT, LA SOMME ET LES COURS D'EAU CÔTIERS DE LA MANCHE ET DE LA MER DU NORD", "MultiPolygon", 50.244544940, 2.654213441, "Surface water status"),
        ("FIWDA", 65, "87%", 75, "water bodies", "Finland", "FIWDA", "FI", "ÅLAND RIVER BASIN DISTRICT", "Polygon", 60.208602103, 20.337884112, "Surface water status"),
        ("FIVHA7", 1, "0%", 460, "water bodies", "Finland", "FIVHA7", "FI", "TENO, NÄÄTÄMÖJOKI AND PAATSJOKI IRBD", "Polygon", 68.717723320, 27.704965466, "Surface water status"),
        ("FIVHA6", 20, "7%", 269, "water bodies", "Finland", "FIVHA6", "FI", "TORNIONJOKI IRBD", "MultiPolygon", 67.621065469, 23.475211254, "Surface water status"),
        ("FIVHA5", 33, "5%", 723, "water bodies", "Finland", "FIVHA5", "FI", "KEMIJOKI RIVER BASIN DISTRICT", "MultiPolygon", 67.158514888, 26.285714938, "Surface water status"),
        ("FIVHA4", 747, "60%", 1249, "water bodies", "Finland", "FIVHA4", "FI", "OULUJOKI-IIJOKI RIVER BASIN DISTRICT", "Polygon", 64.974538124, 27.385075250, "Surface water status"),
        ("FIVHA3", 1045, "88%", 1191, "water bodies", "Finland", "FIVHA3", "FI", "KOKEMÄENJOKI-ARCHIPELAGO SEA-BOTHNIAN SEA RIVER BASIN DISTRICT", "MultiPolygon", 61.893093088, 23.190644414, "Surface water status"),
        ("FIVHA2", 1112, "87%", 1285, "water bodies", "Finland", "FIVHA2", "FI", "KYMIJOKI-GULF OF FINLAND RIVER BASIN DISTRICT", "MultiPolygon", 61.604424801, 25.600837985, "Surface water status"),
        ("FIVHA1", 1351, "90%", 1507, "water bodies", "Finland", "FIVHA1", "FI", "VUOKSI RIVER BASIN DISTRICT", "MultiPolygon", 62.531149245, 28.347140195, "Surface water status"),
        ("ES160", 1, "25%", 4, "water bodies", "Spain", "ES160", "ES", "MELILLA", "Polygon", 35.297665901, -2.940966496, "Surface water status"),
        ("ES150", 1, "33%", 3, "water bodies", "Spain", "ES150", "ES", "CEUTA", "Polygon", 35.901328658, -5.327163575, "Surface water status"),
        ("ES110", 39, "100%", 39, "water bodies", "Spain", "ES110", "ES", "BALEARIC ISLANDS", "MultiPolygon", 39.474252231, 2.776391872, "Surface water status"),
        ("ES100", 204, "68%", 299, "water bodies", "Spain", "ES100", "ES", "INTERNAL BASINS OF CATALONIA", "MultiPolygon", 41.652147528, 1.867833873, "Surface water status"),
        ("ES091", 240, "29%", 816, "water bodies", "Spain", "ES091", "ES", "EBRO", "MultiPolygon", 41.947769658, -1.094773033, "Surface water status"),
        ("ES080", 227, "65%", 349, "water bodies", "Spain", "ES080", "ES", "JUCAR", "MultiPolygon", 39.589148420, -1.045755174, "Surface water status"),
        ("ES070", 54, "47%", 114, "water bodies", "Spain", "ES070", "ES", "SEGURA", "MultiPolygon", 38.133435301, -1.665422541, "Surface water status"),
        ("ES064", 37, "58%", 64, "water bodies", "Spain", "ES064", "ES", "TINTO, ODIEL AND PIEDRAS", "Polygon", 37.508655084, -6.847955542, "Surface water status"),
        ("ES063", 57, "59%", 97, "water bodies", "Spain", "ES063", "ES", "GUADALETE AND BARBATE", "Polygon", 36.585363465, -5.712586838, "Surface water status"),
        ("ES060", 80, "45%", 177, "water bodies", "Spain", "ES060", "ES", "ANDALUSIA MEDITERRANEAN BASINS", "MultiPolygon", 36.885078020, -3.811973154, "Surface water status"),
        ("ES050", 173, "39%", 446, "water bodies", "Spain", "ES050", "ES", "GUADALQUIVIR", "Polygon", 37.745391798, -4.428470817, "Surface water status"),
        ("ES040", 216, "70%", 309, "water bodies", "Spain", "ES040", "ES", "GUADIANA", "MultiPolygon", 38.820393513, -5.118698934, "Surface water status"),
        ("ES030", 135, "43%", 317, "water bodies", "Spain", "ES030", "ES", "TAGUS", "MultiPolygon", 40.153047108, -4.527243239, "Surface water status"),
        ("ES020", 505, "71%", 709, "water bodies", "Spain", "ES020", "ES", "DUERO", "MultiPolygon", 41.691347992, -5.078116247, "Surface water status"),
        ("ES018", 55, "19%", 293, "water bodies", "Spain", "ES018", "ES", "WESTERN CANTABRIAN", "Polygon", 43.240597625, -5.448142206, "Surface water status"),
        ("ES017", 53, "38%", 138, "water bodies", "Spain", "ES017", "ES", "EASTERN CANTABRIAN", "MultiPolygon", 43.154064873, -2.307480734, "Surface water status"),
        ("ES014", 109, "23%", 466, "water bodies", "Spain", "ES014", "ES", "GALICIAN COAST", "MultiPolygon", 42.996367478, -8.248264158, "Surface water status"),
        ("ES010", 67, "24%", 279, "water bodies", "Spain", "ES010", "ES", "MINHO", "MultiPolygon", 42.537593296, -7.470255765, "Surface water status"),
        ("EL14", 5, "3%", 164, "water bodies", "Greece", "EL14", "EL", "AEGEAN ISLANDS", "MultiPolygon", 37.192433813, 26.102463596, "Surface water status"),
        ("EL13", 30, "22%", 135, "water bodies", "Greece", "EL13", "EL", "CRETE", "MultiPolygon", 35.244417748, 24.902926453, "Surface water status"),
        ("EL12", 40, "24%", 168, "water bodies", "Greece", "EL12", "EL", "THRACE", "MultiPolygon", 41.123144225, 25.327959399, "Surface water status"),
        ("EL11", 29, "40%", 73, "water bodies", "Greece", "EL11", "EL", "EASTERN MACEDONIA", "Polygon", 41.068916971, 23.742252234, "Surface water status"),
        ("EL10", 61, "53%", 115, "water bodies", "Greece", "EL10", "EL", "CENTRAL MACEDONIA", "Polygon", 40.559876186, 23.250703179, "Surface water status"),
        ("EL09", 69, "43%", 159, "water bodies", "Greece", "EL09", "EL", "WESTERN MACEDONIA", "MultiPolygon", 40.434665250, 21.816395082, "Surface water status"),
        ("EL08", 42, "58%", 73, "water bodies", "Greece", "EL08", "EL", "THESSALIA", "Polygon", 39.530296952, 22.234230872, "Surface water status"),
        ("EL07", 43, "48%", 89, "water bodies", "Greece", "EL07", "EL", "EASTERN STEREA ELLADA", "MultiPolygon", 38.730114897, 23.546654195, "Surface water status"),
        ("EL06", 19, "70%", 27, "water bodies", "Greece", "EL06", "EL", "ATTICA", "MultiPolygon", 37.827621786, 23.610644050, "Surface water status"),
        ("EL05", 32, "32%", 100, "water bodies", "Greece", "EL05", "EL", "EPIRUS", "MultiPolygon", 39.607409581, 20.446012233, "Surface water status"),
        ("EL04", 24, "21%", 114, "water bodies", "Greece", "EL04", "EL", "WESTERN STEREA ELLADA", "Polygon", 38.889190676, 21.486414902, "Surface water status"),
        ("EL03", 19, "22%", 85, "water bodies", "Greece", "EL03", "EL", "EASTERN PELOPONNESE", "MultiPolygon", 36.966045674, 22.906199351, "Surface water status"),
        ("EL02", 29, "34%", 86, "water bodies", "Greece", "EL02", "EL", "NORTHERN PELOPONNESE", "MultiPolygon", 37.980900308, 21.617677988, "Surface water status"),
        ("EL01", 34, "31%", 108, "water bodies", "Greece", "EL01", "EL", "WESTERN PELOPONNESE", "MultiPolygon", 37.219990760, 22.017020032, "Surface water status"),
        ("EE3", 7, "88%", 8, "water bodies", "Estonia", "EE3", "EE", "KOIVA VESIKOND", "MultiPolygon", 57.663889498, 26.765871042, "Surface water status"),
        ("EE2", 123, "95%", 130, "water bodies", "Estonia", "EE2", "EE", "IDA-EESTI VESIKOND", "MultiPolygon", 58.695874183, 26.682152655, "Surface water status"),
        ("EE1", 170, "94%", 181, "water bodies", "Estonia", "EE1", "EE", "LÄÄNE-EESTI VESIKOND", "MultiPolygon", 58.639054544, 24.116267796, "Surface water status"),
        ("DK4", 67, "100%", 67, "water bodies", "Denmark", "DK4", "DK", "VIDAA-KRUSAA", "Polygon", 54.994945544, 8.986209157, "Surface water status"),
        ("DK3", 44, "100%", 44, "water bodies", "Denmark", "DK3", "DK", "BORNHOLM", "MultiPolygon", 55.157414347, 14.945091555, "Surface water status"),
        ("DK2", 897, "100%", 899, "water bodies", "Denmark", "DK2", "DK", "SEALAND", "MultiPolygon", 55.403420576, 11.808304246, "Surface water status"),
        ("DK1", 3196, "100%", 3197, "water bodies", "Denmark", "DK1", "DK", "JUTLAND AND FUNEN", "MultiPolygon", 56.192381009, 9.859868724, "Surface water status"),
        ("DE9650", 598, "100%", 598, "water bodies", "Germany", "DE9650", "DE", "WARNOW/PEENE", "Polygon", 53.995321699, 12.703281371, "Surface water status"),
        ("DE9610", 348, "100%", 348, "water bodies", "Germany", "DE9610", "DE", "SCHLEI/TRAVE", "Polygon", 54.237822757, 10.476236381, "Surface water status"),
        ("DE9500", 163, "100%", 163, "water bodies", "Germany", "DE9500", "DE", "EIDER", "Polygon", 54.535243825, 8.887172030, "Surface water status"),
        ("DE7000", 229, "100%", 229, "water bodies", "Germany", "DE7000", "DE", "MAAS", "Polygon", 51.066335346, 6.283008210, "Surface water status"),
        ("DE6000", 501, "100%", 501, "water bodies", "Germany", "DE6000", "DE", "ODER", "MultiPolygon", 52.455977778, 14.286997301, "Surface water status"),
        ("DE5000", 3146, "100%", 3146, "water bodies", "Germany", "DE5000", "DE", "ELBE", "MultiPolygon", 52.278489865, 11.626160060, "Surface water status"),
        ("DE4000", 1438, "100%", 1438, "water bodies", "Germany", "DE4000", "DE", "WESER", "MultiPolygon", 52.056459528, 9.495776001, "Surface water status"),
        ("DE3000", 495, "100%", 495, "water bodies", "Germany", "DE3000", "DE", "EMS", "Polygon", 52.825127703, 7.685229619, "Surface water status"),
        ("DE2000", 2169, "100%", 2169, "water bodies", "Germany", "DE2000", "DE", "RHEIN", "MultiPolygon", 49.682706727, 8.650357240, "Surface water status"),
        ("DE1000", 721, "100%", 721, "water bodies", "Germany", "DE1000", "DE", "DONAU", "Polygon", 48.427685664, 11.066668299, "Surface water status"),
        ("CZ6000", 112, "81%", 138, "water bodies", "Czechia", "CZ6000", "CZ", "ODRA", "MultiPolygon", 50.238135502, 16.939911471, "Surface water status"),
        ("CZ5000", 593, "88%", 671, "water bodies", "Czechia", "CZ5000", "CZ", "ELBE", "MultiPolygon", 49.937230228, 14.484550981, "Surface water status"),
        ("CZ1000", 234, "80%", 293, "water bodies", "Czechia", "CZ1000", "CZ", "DANUBE", "MultiPolygon", 49.371761895, 15.903763110, "Surface water status"),
        ("CY001", 82, "42%", 197, "water bodies", "Cyprus", "CY001", "CY", "CYPRUS", "MultiPolygon", 35.143145463, 33.425464281, "Surface water status"),
        ("BG4000", 64, "52%", 124, "water bodies", "Bulgaria", "BG4000", "BG", "WEST AEGEAN", "Polygon", 42.025460154, 23.268887055, "Surface water status"),
        ("BG3000", 150, "96%", 157, "water bodies", "Bulgaria", "BG3000", "BG", "EAST AEGEAN", "MultiPolygon", 42.149429417, 25.368811913, "Surface water status"),
        ("BG2000", 129, "93%", 139, "water bodies", "Bulgaria", "BG2000", "BG", "BLACK SEA", "MultiPolygon", 42.874414989, 27.391958086, "Surface water status"),
        ("BG1000", 89, "46%", 193, "water bodies", "Bulgaria", "BG1000", "BG", "DANUBE", "Polygon", 43.334158032, 24.892702887, "Surface water status"),
        ("BESEINE_RW", 2, "100%", 2, "water bodies", "Belgium", "BESEINE_RW", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE SEINE", "Polygon", 49.989398226, 4.254550672, "Surface water status"),
        ("BESCHELDE_VL", 176, "100%", 176, "water bodies", "Belgium", "BESCHELDE_VL", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE SCHELDT", "MultiPolygon", 51.004781540, 4.050155601, "Surface water status"),
        ("BERHIN_RW", 16, "100%", 16, "water bodies", "Belgium", "BERHIN_RW", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE RHINE", "MultiPolygon", 50.011666116, 5.914955994, "Surface water status"),
        ("BENOORDZEE_FED", 2, "100%", 2, "water bodies", "Belgium", "BENOORDZEE_FED", "BE", "BELGIAN COASTAL ZONE", "Polygon", 51.254520528, 2.941697957, "Surface water status"),
        ("BEMEUSE_RW", 257, "100%", 257, "water bodies", "Belgium", "BEMEUSE_RW", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE MEUSE", "MultiPolygon", 50.179303721, 5.240169980, "Surface water status"),
        ("BEMAAS_VL", 18, "100%", 18, "water bodies", "Belgium", "BEMAAS_VL", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE MEUSE", "MultiPolygon", 51.143096842, 5.266570035, "Surface water status"),
        ("BEESCAUT_SCHELDE_BR", 3, "100%", 3, "water bodies", "Belgium", "BEESCAUT_SCHELDE_BR", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE SCHELDT", "Polygon", 50.834852521, 4.373913458, "Surface water status"),
        ("BEESCAUT_RW", 79, "100%", 79, "water bodies", "Belgium", "BEESCAUT_RW", "BE", "INTERNATIONAL RIVER BASIN DISTRICT OF THE SCHELDT", "MultiPolygon", 50.628074122, 4.042822587, "Surface water status"),
        ("AT5000", 101, "100%", 101, "water bodies", "Austria", "AT5000", "AT", "ELBE", "MultiPolygon", 48.704632417, 14.713672849, "Surface water status"),
        ("AT2000", 219, "100%", 219, "water bodies", "Austria", "AT2000", "AT", "RHINE", "MultiPolygon", 47.222592687, 9.901488475, "Surface water status"),
        ("AT1000", 7807, "100%", 7807, "water bodies", "Austria", "AT1000", "AT", "DANUBE", "MultiPolygon", 47.576229624, 13.692901618, "Surface water status")
    ]

    # **Insert data into the table**
    insert_query = """
    INSERT INTO swRBD_Europe_data (
        euRBDCode_, C_StatusFailing, C_StatusFailingPercent, C_StatusKnown, C_Unit,
        countryName, euRBDCode, NUTS0, rbdName, geography, Latitude, Longitude, P_StatusFailing
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    cur.executemany(insert_query, data)

    # **Commit changes and close connection**
    conn.commit()
    conn.close()

    print("✅ Table 'swRBD_Europe_data' successfully created and populated!")

def rbdCodeNames(db_file, countryCode, cYear, working_directory):
    """
    Extracts and saves RBD Codes and Names for specified countries.
    """

    output_file = os.path.join(working_directory, f"rbdCodeNames{cYear}.csv")
    headers = ["Country", "RBD Code", "RBD Name"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    try:
        cur = conn.cursor()

        # Constructing SQL query with placeholders
        query = '''
            SELECT DISTINCT NUTS0, euRBDCode, rbdName
            FROM swRBD_Europe_data
            WHERE NUTS0 IN ({})
            ORDER BY euRBDCode;
        '''.format(', '.join(['?'] * len(countryCode)))

        cur.execute(query, countryCode)
        data = cur.fetchall()

        # **Write to CSV**
        with open(output_file, 'w+', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data)

        print(f"✅ Data successfully written to {output_file}")

    except sqlite3.Error as e:
        print(f"❌ Error executing query: {e}")

    finally:
        conn.close()

def WISE_SOW_SurfaceWaterBody_SWB_Table(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract surface water body statistics with accurate calculations."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"1.surfaceWaterBodyNumberAndSite{cYear}.csv")

    headers = ['Country', 'Year', 'Number', 'Number (%)', 'Length (km)', 'Length (%)', 
               'Area (km^2)', 'Area (%)', 'Median Length (km)', 'Median Area (km^2)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, euSurfaceWaterBodyCode, cLength, cArea
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
            AND countryCode IN ({','.join(['?'] * len(countryCode))})
        ),
        TotalCounts AS (
            SELECT countryCode, 
                   COUNT(euSurfaceWaterBodyCode) AS total_count, 
                   SUM(cLength) AS total_length, 
                   SUM(cArea) AS total_area
            FROM FilteredData
            GROUP BY countryCode
        ),
        MedianLength AS (
            SELECT countryCode, AVG(cLength) AS median_length
            FROM (
                SELECT countryCode, cLength, 
                       ROW_NUMBER() OVER (PARTITION BY countryCode ORDER BY cLength) AS row_num,
                       COUNT(*) OVER (PARTITION BY countryCode) AS total_count
                FROM FilteredData
                WHERE cLength IS NOT NULL
            )
            WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count / 2) + 1
            GROUP BY countryCode
        ),
        MedianArea AS (
            SELECT countryCode, AVG(cArea) AS median_area
            FROM (
                SELECT countryCode, cArea, 
                       ROW_NUMBER() OVER (PARTITION BY countryCode ORDER BY cArea) AS row_num,
                       COUNT(*) OVER (PARTITION BY countryCode) AS total_count
                FROM FilteredData
                WHERE cArea IS NOT NULL
            )
            WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count / 2) + 1
            GROUP BY countryCode
        ),
        AggregatedData AS (
            SELECT f.countryCode, ? AS cYear,
                   COUNT(f.euSurfaceWaterBodyCode) AS num_swb,
                   ROUND(COUNT(f.euSurfaceWaterBodyCode) * 100.0 / t.total_count, 0) AS num_percent,
                   ROUND(SUM(f.cLength), 0) AS total_length_km,
                   ROUND(SUM(f.cLength) * 100.0 / t.total_length, 0) AS length_percent,
                   ROUND(SUM(f.cArea), 0) AS total_area_km2,
                   ROUND(SUM(f.cArea) * 100.0 / t.total_area, 0) AS area_percent
            FROM FilteredData f
            JOIN TotalCounts t ON f.countryCode = t.countryCode
            GROUP BY f.countryCode
        )
        SELECT a.*, 
               ROUND(COALESCE(mL.median_length, 0), 0) AS median_length, 
               ROUND(COALESCE(mA.median_area, 0), 0) AS median_area
        FROM AggregatedData a
        LEFT JOIN MedianLength mL ON a.countryCode = mL.countryCode
        LEFT JOIN MedianArea mA ON a.countryCode = mA.countryCode
        ORDER BY a.countryCode;
    """

    # Execute query with parameters
    cur.execute(query, [cYear] + countryCode + [cYear])
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def WISE_SOW_SurfaceWaterBody_SWB_Category(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract surface water body category statistics using CTEs."""
    
    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"3.surfaceWaterBodyCategory{cYear}.csv")

    headers = ["Country", "Year", "Surface Water Body Category", "Type", "Total"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Define categories and types
    WDFCode = ["RW", "LW", "TW", "CW", "TeW"]
    naturalAWBHMWB = ["Natural water body", "Heavily modified water body", "Artificial water body"]

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, surfaceWaterBodyCategory, naturalAWBHMWB
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
            AND countryCode IN ({','.join('?' * len(countryCode))})
            AND surfaceWaterBodyCategory IN ({','.join('?' * len(WDFCode))})
            AND naturalAWBHMWB IN ({','.join('?' * len(naturalAWBHMWB))})
        ),
        CategoryCounts AS (
            SELECT countryCode, surfaceWaterBodyCategory, naturalAWBHMWB, COUNT(*) AS total
            FROM FilteredData
            GROUP BY countryCode, surfaceWaterBodyCategory, naturalAWBHMWB
        )
        SELECT cc.countryCode, ? AS cYear, cc.surfaceWaterBodyCategory, cc.naturalAWBHMWB, cc.total
        FROM CategoryCounts cc
        ORDER BY cc.countryCode, cc.surfaceWaterBodyCategory, cc.naturalAWBHMWB;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + WDFCode + naturalAWBHMWB + [cYear])
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Surface_water_bodies_Ecological_exemptions_and_pressures(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract ecological exemption and pressure data using CTEs."""
    
    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"6.Surface_water_bodies_Ecological_exemptions_and_pressures{cYear}.csv")

    headers = ["Country", "Year", "Ecological Exemption Type Group",
               "Ecological Exemption Type", "Ecological Exemption Pressure Group",
               "Ecological Exemption Pressure", "Number"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, euSurfaceWaterBodyCode, swEcologicalExemptionTypeGroup,
                   swEcologicalExemptionType, swEcologicalExemptionPressureGroup,
                   swEcologicalExemptionPressure
            FROM SOW_SWB_SWE_swEcologicalExemptionPressure
            WHERE cYear = ?
            AND countryCode IN ({','.join('?' * len(countryCode))})
        ),
        ExemptionPressureCounts AS (
            SELECT countryCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType,
                   swEcologicalExemptionPressureGroup, swEcologicalExemptionPressure,
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS total
            FROM FilteredData
            GROUP BY countryCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType,
                     swEcologicalExemptionPressureGroup, swEcologicalExemptionPressure
        )
        SELECT epc.countryCode, ? AS cYear, epc.swEcologicalExemptionTypeGroup, epc.swEcologicalExemptionType, 
               epc.swEcologicalExemptionPressureGroup, epc.swEcologicalExemptionPressure, epc.total
        FROM ExemptionPressureCounts epc
        ORDER BY epc.countryCode, epc.swEcologicalExemptionTypeGroup, epc.swEcologicalExemptionType, 
                 epc.swEcologicalExemptionPressureGroup, epc.swEcologicalExemptionPressure;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + [cYear])
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Surface_water_bodies_Ecological_exemptions_Type(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract ecological exemption type data using CTEs."""
    
    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"6.Surface_water_bodies_Ecological_exemptions_Type{cYear}.csv")

    headers = ["Country", "Year", "Ecological Exemption Type Group", "Ecological Exemption Type", "Number", "Number (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # List of exemption types to filter
    swEcologicalExemptionTypeGroup = ["Article4(4)", "Article4(5)", "Article4(6)", "Article4(7)"]

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, euSurfaceWaterBodyCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType
            FROM SOW_SWB_SWEcologicalExemptionType
            WHERE cYear = ?
            AND countryCode IN ({','.join('?' * len(countryCode))})
            AND swEcologicalExemptionTypeGroup IN ({','.join('?' * len(swEcologicalExemptionTypeGroup))})
            AND swEcologicalExemptionTypeGroup NOT IN ("None", "Unpopulated")
        ),
        TotalCounts AS (
            SELECT countryCode, swEcologicalExemptionTypeGroup, COUNT(DISTINCT euSurfaceWaterBodyCode) AS total
            FROM FilteredData
            GROUP BY countryCode, swEcologicalExemptionTypeGroup
        ),
        ExemptionCounts AS (
            SELECT countryCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType,
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS Number
            FROM FilteredData
            GROUP BY countryCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType
        )
        SELECT ec.countryCode, ? AS cYear, ec.swEcologicalExemptionTypeGroup, ec.swEcologicalExemptionType, 
               ec.Number,
               ROUND(ec.Number * 100.0 / tc.total, 2) AS "Number (%)"
        FROM ExemptionCounts ec
        JOIN TotalCounts tc 
        ON ec.countryCode = tc.countryCode AND ec.swEcologicalExemptionTypeGroup = tc.swEcologicalExemptionTypeGroup
        ORDER BY ec.countryCode, ec.swEcologicalExemptionTypeGroup, ec.swEcologicalExemptionType;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + swEcologicalExemptionTypeGroup + [cYear])
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Surface_water_bodies_Ecological_exemptions_Type(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract ecological exemption type data using CTEs."""
    
    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"6.Surface_water_bodies_Ecological_exemptions_Type{cYear}.csv")

    headers = ["Country", "Year", "Ecological Exemption Type Group", "Ecological Exemption Type", "Number", "Number (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # List of exemption types to filter
    swEcologicalExemptionTypeGroup = ["Article4(4)", "Article4(5)", "Article4(6)", "Article4(7)"]

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, euSurfaceWaterBodyCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType
            FROM SOW_SWB_SWEcologicalExemptionType
            WHERE cYear = ?
            AND countryCode IN ({','.join('?' * len(countryCode))})
            AND swEcologicalExemptionTypeGroup IN ({','.join('?' * len(swEcologicalExemptionTypeGroup))})
            AND swEcologicalExemptionTypeGroup NOT IN ("None", "Unpopulated")
        ),
        TotalCounts AS (
            SELECT countryCode, swEcologicalExemptionTypeGroup, COUNT(DISTINCT euSurfaceWaterBodyCode) AS total
            FROM FilteredData
            GROUP BY countryCode, swEcologicalExemptionTypeGroup
        ),
        ExemptionCounts AS (
            SELECT countryCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType,
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS Number
            FROM FilteredData
            GROUP BY countryCode, swEcologicalExemptionTypeGroup, swEcologicalExemptionType
        )
        SELECT ec.countryCode, ? AS cYear, ec.swEcologicalExemptionTypeGroup, ec.swEcologicalExemptionType, 
               ec.Number,
               ROUND(ec.Number * 100.0 / tc.total, 2) AS "Number (%)"
        FROM ExemptionCounts ec
        JOIN TotalCounts tc 
        ON ec.countryCode = tc.countryCode AND ec.swEcologicalExemptionTypeGroup = tc.swEcologicalExemptionTypeGroup
        ORDER BY ec.countryCode, ec.swEcologicalExemptionTypeGroup, ec.swEcologicalExemptionType;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + swEcologicalExemptionTypeGroup + [cYear])
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Surface_water_bodies_Quality_element_exemptions_Type(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract quality element exemptions data with accurate percentage calculations."""

    output_file = os.path.join(working_directory, f"6.Surface_water_bodies_Quality_element_exemptions_Type{cYear}.csv")

    headers = ["Country", "Year", "Quality Element Exemption Type Group", 
               "Quality Element Exemption Type", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return
    
    cur = conn.cursor()

    # List of exemption types to filter
    swEcologicalExemptionTypeGroup = ["Article4(4)", "Article4(5)", "Article4(6)", "Article4(7)"]

    # SQL Query with improved percentage calculation
    query = f"""
        WITH total_counts AS (
            SELECT countryCode, qeEcologicalExemptionTypeGroup, COUNT(DISTINCT euSurfaceWaterBodyCode) AS total
            FROM SOW_SWB_QE_qeEcologicalExemptionType
            WHERE cYear = ?
            AND countryCode IN ({','.join('?' * len(countryCode))})
            AND qeEcologicalExemptionTypeGroup IN ({','.join('?' * len(swEcologicalExemptionTypeGroup))})
            GROUP BY countryCode, qeEcologicalExemptionTypeGroup
        )
        SELECT e.countryCode, e.cYear, e.qeEcologicalExemptionTypeGroup, e.qeEcologicalExemptionType,
               COUNT(DISTINCT e.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(DISTINCT e.euSurfaceWaterBodyCode) * 100.0 / t.total, 2
               ) AS "Number(%)"
        FROM SOW_SWB_QE_qeEcologicalExemptionType e
        JOIN total_counts t
        ON e.countryCode = t.countryCode AND e.qeEcologicalExemptionTypeGroup = t.qeEcologicalExemptionTypeGroup
        WHERE e.cYear = ?
        AND e.countryCode IN ({','.join('?' * len(countryCode))})
        AND e.qeEcologicalExemptionTypeGroup IN ({','.join('?' * len(swEcologicalExemptionTypeGroup))})
        AND e.qeEcologicalExemptionTypeGroup NOT IN ("None", "Unpopulated")
        GROUP BY e.countryCode, e.cYear, e.qeEcologicalExemptionTypeGroup, e.qeEcologicalExemptionType, t.total;
    """

    # Execute query with parameters
    cur.execute(query, [cYear] + countryCode + swEcologicalExemptionTypeGroup + [cYear] + countryCode + swEcologicalExemptionTypeGroup)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def SWB_Chemical_exemption_type(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract chemical exemption type data using CTEs with accurate percentage calculations."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"6.swChemical_exemption_type{cYear}.csv")

    headers = ["Country", "Chemical Exemption Type Group", "Chemical Exemption Type", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT DISTINCT countryCode, euSurfaceWaterBodyCode, surfaceWaterBodyCategory,
                            swChemicalExemptionTypeGroup, swChemicalExemptionType, cArea
            FROM SOW_SWB_SWP_SWChemicalExemptionType
            WHERE cYear = ?
            AND countryCode IN ({','.join('?' * len(countryCode))})
            AND swEcologicalStatusOrPotentialValue <> 'unknown'
            AND naturalAWBHMWB <> 'unpopulated'
            AND swChemicalStatusValue <> 'unpopulated'
        ),
        TotalCountryArea AS (
            SELECT countryCode, SUM(cArea) AS total_area
            FROM FilteredData
            GROUP BY countryCode
        ),
        ExemptionArea AS (
            SELECT countryCode, swChemicalExemptionTypeGroup, swChemicalExemptionType, SUM(cArea) AS exemption_area
            FROM FilteredData
            GROUP BY countryCode, swChemicalExemptionTypeGroup, swChemicalExemptionType
        )
        SELECT ea.countryCode, 
               ea.swChemicalExemptionTypeGroup, 
               ea.swChemicalExemptionType,
               ROUND(ea.exemption_area, 0) AS "Area (km^2)",
               CASE 
                   WHEN tc.total_area > 0 THEN ROUND(ea.exemption_area * 100.0 / tc.total_area, 0)
                   ELSE 0
               END AS "Area (%)"
        FROM ExemptionArea ea
        JOIN TotalCountryArea tc 
        ON ea.countryCode = tc.countryCode
        ORDER BY ea.countryCode, ea.swChemicalExemptionTypeGroup, ea.swChemicalExemptionType;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def WISE_SOW_SurfaceWaterBody_SWB_ChemicalStatus_Table(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract chemical status values from the database using CTEs."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"12.surfaceWaterBodyChemicalStatusGood{cYear}.csv")

    headers = ['Country', 'Year', "Chemical Status Value", 'Number', 'Number(%)', 'Length (km)', 
               'Length(%)', 'Area (km^2)', 'Area(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Define valid chemical status values
    swChemicalStatusValue = ["2", "3"]

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, euSurfaceWaterBodyCode, cYear, swChemicalStatusValue, cLength, cArea, surfaceWaterBodyCategory
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swChemicalStatusValue IN ({','.join('?' * len(swChemicalStatusValue))})
              AND swChemicalStatusValue NOT IN ('unknown', 'Unpopulated')
              AND surfaceWaterBodyCategory <> 'Unpopulated'
              AND naturalAWBHMWB NOT IN ('Unpopulated', 'Unknown')
        ),
        ChemicalStatus AS (
            SELECT countryCode, cYear, swChemicalStatusValue,
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS total_count,
                   SUM(cLength) AS total_length,
                   SUM(CASE WHEN surfaceWaterBodyCategory <> 'RW' AND cArea IS NOT NULL THEN cArea END) AS total_area
            FROM FilteredData
            GROUP BY countryCode, cYear, swChemicalStatusValue
        ),
        TotalValues AS (
            SELECT countryCode, 
                   SUM(total_count) AS grand_total_count,
                   SUM(total_length) AS grand_total_length,
                   SUM(total_area) AS grand_total_area
            FROM ChemicalStatus
            GROUP BY countryCode
        )
        SELECT cs.countryCode, 
               cs.cYear, 
               cs.swChemicalStatusValue,
               cs.total_count,
               ROUND(cs.total_count * 100.0 / NULLIF(tv.grand_total_count, 0), 0) AS "Number(%)",
               ROUND(cs.total_length, 0) AS "Length (km)",
               ROUND(cs.total_length * 100.0 / NULLIF(tv.grand_total_length, 0), 0) AS "Length(%)",
               ROUND(cs.total_area, 0) AS "Area (km^2)",
               ROUND(cs.total_area * 100.0 / NULLIF(tv.grand_total_area, 0), 0) AS "Area(%)"
        FROM ChemicalStatus cs
        JOIN TotalValues tv ON cs.countryCode = tv.countryCode
        ORDER BY cs.countryCode, cs.swChemicalStatusValue;
    """

    # Execute query with parameters
    cur.execute(query, [cYear] + countryCode + swChemicalStatusValue)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()              

def SurfaceWaterBody_ChemicalStatus_Table_by_Category(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract chemical status values by category using CTEs."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"12.SurfaceWaterBody_SWB_ChemicalStatus_Table_by_Category{cYear}.csv")

    headers = ['Country', 'Year', 'Surface Water Body Category', 'Chemical Status Value', 'Number', 'Number(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Define valid categories and chemical statuses
    WDFCode = ["RW", "LW", "TW", "CW", "TeW"]
    chemicalStatus = ["2", "3", "unknown"]

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swChemicalStatusValue
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swChemicalStatusValue NOT IN ('Unpopulated')
              AND surfaceWaterBodyCategory NOT IN ('Unpopulated')
              AND surfaceWaterBodyCategory IN ({','.join('?' * len(WDFCode))})
        ),
        CategoryCounts AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swChemicalStatusValue,
                   COUNT(*) AS total_count,
                   SUM(COUNT(*)) OVER (PARTITION BY countryCode, cYear, surfaceWaterBodyCategory) AS category_total
            FROM FilteredData
            GROUP BY countryCode, cYear, surfaceWaterBodyCategory, swChemicalStatusValue
        )
        SELECT cc.countryCode, 
               cc.cYear, 
               cc.surfaceWaterBodyCategory, 
               cc.swChemicalStatusValue,
               cc.total_count,
               ROUND(cc.total_count * 100.0 / NULLIF(cc.category_total, 0), 0) AS "Number(%)"
        FROM CategoryCounts cc
        WHERE cc.swChemicalStatusValue IN ({','.join('?' * len(chemicalStatus))})
        ORDER BY cc.countryCode, cc.surfaceWaterBodyCategory, cc.swChemicalStatusValue;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + WDFCode + chemicalStatus)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Surface_water_bodies_Ecological_status_or_potential_groupGoodHigh(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract ecological status or potential for 'Good' and 'High' groups using CTEs."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"8.Surface_water_bodies_Ecological_status_or_potential_group_Good_High{cYear}.csv")

    headers = ["Country", "Year", "Number", "Number(%)", "Length (km)", "Length(%)", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, swEcologicalStatusOrPotentialValue, cLength, cArea, surfaceWaterBodyCategory, naturalAWBHMWB
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swEcologicalStatusOrPotentialValue IN ('1', '2')
              AND swEcologicalStatusOrPotentialValue NOT IN ('inapplicable', 'unknown', 'Other')
              AND surfaceWaterBodyCategory NOT IN ('Unpopulated')
              AND naturalAWBHMWB NOT IN ('Unknown', 'Unpopulated')
        ),
        TotalCounts AS (
            SELECT countryCode, cYear,
                   COUNT(*) AS total_count,
                   SUM(CASE WHEN surfaceWaterBodyCategory = 'RW' THEN cLength END) AS total_length,
                   SUM(CASE WHEN surfaceWaterBodyCategory <> 'RW' THEN cArea END) AS total_area
            FROM FilteredData
            GROUP BY countryCode, cYear
        ),
        GoodHighCounts AS (
            SELECT countryCode, cYear,
                   COUNT(*) AS num_status,
                   SUM(CASE WHEN surfaceWaterBodyCategory = 'RW' THEN cLength END) AS total_length_rw,
                   SUM(CASE WHEN surfaceWaterBodyCategory <> 'RW' THEN cArea END) AS total_area_other
            FROM FilteredData
            GROUP BY countryCode, cYear
        )
        SELECT ghc.countryCode,
               ghc.cYear,
               ghc.num_status,
               ROUND(ghc.num_status * 100.0 / NULLIF(tc.total_count, 0), 0) AS "Number(%)",
               ROUND(ghc.total_length_rw, 0) AS "Length (km)",
               ROUND(ghc.total_length_rw * 100.0 / NULLIF(tc.total_length, 0), 0) AS "Length(%)",
               ROUND(ghc.total_area_other, 0) AS "Area (km^2)",
               ROUND(ghc.total_area_other * 100.0 / NULLIF(tc.total_area, 0), 0) AS "Area(%)"
        FROM GoodHighCounts ghc
        JOIN TotalCounts tc ON ghc.countryCode = tc.countryCode AND ghc.cYear = tc.cYear
        ORDER BY ghc.countryCode;
    """

    # Execute query with parameters
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Surface_water_bodies_Ecological_status_or_potential_groupFailling(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract ecological status or potential for 'Failing' groups using CTEs."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"8.Surface_water_bodies_Ecological_status_or_potential_group_Failing{cYear}.csv")

    headers = ["Country", "Year", "Number", "Number(%)", "Length (km)", "Length(%)", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, swEcologicalStatusOrPotentialValue, cLength, cArea, surfaceWaterBodyCategory, naturalAWBHMWB
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swEcologicalStatusOrPotentialValue IN ('3', '4', '5')
              AND swEcologicalStatusOrPotentialValue NOT IN ('inapplicable', 'unknown', 'Other')
              AND surfaceWaterBodyCategory NOT IN ('Unpopulated')
              AND naturalAWBHMWB NOT IN ('Unknown', 'Unpopulated')
        ),
        TotalCounts AS (
            SELECT countryCode, cYear,
                   COUNT(*) AS total_count,
                   SUM(CASE WHEN surfaceWaterBodyCategory = 'RW' THEN cLength END) AS total_length,
                   SUM(CASE WHEN surfaceWaterBodyCategory <> 'RW' THEN cArea END) AS total_area
            FROM FilteredData
            GROUP BY countryCode, cYear
        ),
        FailingCounts AS (
            SELECT countryCode, cYear,
                   COUNT(*) AS num_status,
                   SUM(CASE WHEN surfaceWaterBodyCategory = 'RW' THEN cLength END) AS total_length_rw,
                   SUM(CASE WHEN surfaceWaterBodyCategory <> 'RW' THEN cArea END) AS total_area_other
            FROM FilteredData
            GROUP BY countryCode, cYear
        )
        SELECT fc.countryCode,
               fc.cYear,
               fc.num_status,
               ROUND(fc.num_status * 100.0 / NULLIF(tc.total_count, 0), 0) AS "Number(%)",
               ROUND(fc.total_length_rw, 0) AS "Length (km)",
               ROUND(fc.total_length_rw * 100.0 / NULLIF(tc.total_length, 0), 0) AS "Length(%)",
               ROUND(fc.total_area_other, 0) AS "Area (km^2)",
               ROUND(fc.total_area_other * 100.0 / NULLIF(tc.total_area, 0), 0) AS "Area(%)"
        FROM FailingCounts fc
        JOIN TotalCounts tc ON fc.countryCode = tc.countryCode AND fc.cYear = tc.cYear
        ORDER BY fc.countryCode;
    """

    # Execute query with parameters
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()            

def swEcologicalStatusOrPotential_RW_LW_Category2ndRBMP2016(db_file, countryCode, cYear, working_directory):
    """Optimized function to extract Ecological Status or Potential by Surface Water Body Category using CTEs."""

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, "8.swEcologicalStatusOrPotential_RW_LW_Category2ndRBMP2016.csv")

    headers = ["Country", "Year", "Surface Water Body Category", "Ecological Status Or Potential Value", "Number"]

    # Define valid categories and ecological status values
    WDFCode = ["RW", "LW", "TW", "CW", "TeW"]
    swEcologicalStatusOrPotentialValue = ["1", "2", "3", "4", "5", "unknown"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND surfaceWaterBodyCategory IN ({','.join('?' * len(WDFCode))})
              AND swEcologicalStatusOrPotentialValue IN ({','.join('?' * len(swEcologicalStatusOrPotentialValue))})
              AND surfaceWaterBodyCategory <> 'unpopulated'
        ),
        StatusCounts AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue,
                   COUNT(*) AS num_records
            FROM FilteredData
            GROUP BY countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue
        )
        SELECT sc.countryCode, 
               sc.cYear, 
               sc.surfaceWaterBodyCategory, 
               sc.swEcologicalStatusOrPotentialValue,
               sc.num_records
        FROM StatusCounts sc
        ORDER BY sc.countryCode, sc.surfaceWaterBodyCategory, sc.swEcologicalStatusOrPotentialValue;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + WDFCode + swEcologicalStatusOrPotentialValue)
    data = cur.fetchall()

    # Write results to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swEcologicalStatusOrPotential_Unknown_Category2ndRBMP2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts Surface Water Body Category with 'Unknown' Ecological Status or Potential using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"9.swEcologicalStatusOrPotential_Unknown_Category2ndRBMP{cYear}.csv")

    headers = ["Country", "Year", "Surface Water Body Category", "Ecological Status Or Potential Value", "Number"]

    # Define valid categories and unknown status value
    WDFCode = ["RW", "LW", "TW", "CW"]
    swEcologicalStatusOrPotentialValue = ["unknown"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs (Removing Redundant Steps)
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue, COUNT(*) AS num_records
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND surfaceWaterBodyCategory IN ({','.join('?' * len(WDFCode))})
              AND swEcologicalStatusOrPotentialValue = ?
            GROUP BY countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue
        )
        SELECT countryCode, 
               cYear, 
               surfaceWaterBodyCategory, 
               swEcologicalStatusOrPotentialValue,
               num_records
        FROM FilteredData
        ORDER BY countryCode, surfaceWaterBodyCategory;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + WDFCode + swEcologicalStatusOrPotentialValue)
    data = cur.fetchall()

    # **Write results to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swEcologicalStatusOrPotentialChemical_by_Country(db_file, countryCode, cYear, working_directory):
    """
    Extracts Surface Water Body Ecological & Chemical Status by Country using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    # Output files
    eco_output_file = os.path.join(working_directory, f"15.swEcologicalStatusOrPotential_by_Country{cYear}.csv")
    chem_output_file = os.path.join(working_directory, f"15.swChemicalStatusValue_by_Country{cYear}.csv")

    # Headers
    eco_headers = ["Country", "Ecological Status Or Potential Value", "Number", "Number(%)"]
    chem_headers = ["Country", "Chemical Status Value", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Ecological Status Query Using CTEs**
    eco_query = f"""
        WITH FilteredData AS (
            SELECT countryCode, swEcologicalStatusOrPotentialValue
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND naturalAWBHMWB <> 'Unpopulated'
        ),
        TotalCounts AS (
            SELECT countryCode, COUNT(*) AS total_count
            FROM FilteredData
            GROUP BY countryCode
        ),
        StatusCounts AS (
            SELECT countryCode, swEcologicalStatusOrPotentialValue,
                   COUNT(*) AS num_records
            FROM FilteredData
            GROUP BY countryCode, swEcologicalStatusOrPotentialValue
        )
        SELECT sc.countryCode, 
               sc.swEcologicalStatusOrPotentialValue,
               sc.num_records AS Number,
               ROUND(sc.num_records * 100.0 / NULLIF(tc.total_count, 0), 0) AS Number_Percent
        FROM StatusCounts sc
        JOIN TotalCounts tc 
          ON sc.countryCode = tc.countryCode
        ORDER BY sc.countryCode, sc.swEcologicalStatusOrPotentialValue;
    """

    cur.execute(eco_query, [cYear] + countryCode)
    eco_data = cur.fetchall()

    # **Write Ecological Status Data to CSV**
    with open(eco_output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(eco_headers)
        writer.writerows(eco_data)

    
    # 🚀 **Optimized Chemical Status Query Using CTEs**
    chem_query = f"""
        WITH FilteredData AS (
            SELECT countryCode, swChemicalStatusValue
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND naturalAWBHMWB <> 'Unpopulated'
        ),
        TotalCounts AS (
            SELECT countryCode, COUNT(*) AS total_count
            FROM FilteredData
            GROUP BY countryCode
        ),
        StatusCounts AS (
            SELECT countryCode, swChemicalStatusValue,
                   COUNT(*) AS num_records
            FROM FilteredData
            GROUP BY countryCode, swChemicalStatusValue
        )
        SELECT sc.countryCode, 
               sc.swChemicalStatusValue,
               sc.num_records AS Number,
               ROUND(sc.num_records * 100.0 / NULLIF(tc.total_count, 0), 0) AS Number_Percent
        FROM StatusCounts sc
        JOIN TotalCounts tc 
          ON sc.countryCode = tc.countryCode
        ORDER BY sc.countryCode, sc.swChemicalStatusValue;
    """

    cur.execute(chem_query, [cYear] + countryCode)
    chem_data = cur.fetchall()

    # **Write Chemical Status Data to CSV**
    with open(chem_output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(chem_headers)
        writer.writerows(chem_data)

    conn.close()
 
def swEcologicalStatusOrPotentialValue_swChemicalStatusValue_by_Country_by_Categ(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Surface Water Ecological & Chemical Status by Category for each Country using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    # Output files
    eco_output_file = os.path.join(working_directory, f"15.swEcologicalStatusOrPotentialValue_swChemicalStatusValue_by_Country_by_Categ.csv")
    chem_output_file = os.path.join(working_directory, f"15.swChemicalStatusValue_by_Country_by_Categ{cYear}.csv")

    # Headers
    eco_headers = ['Country', 'Year', 'Categories', 'Ecological Status Value', 'Number', 'Number(%)']
    chem_headers = ['Country', 'Year', 'Categories', 'Chemical Status Value', 'Number', 'Number(%)']

    # Define valid categories and status values
    WDFCode = ["RW", "LW", "CW", "TW", "TeW"]
    ecoStatus = ["1", "2", "3", "4", "5", "unknown"]
    chemStatus = ["2", "3", "unknown"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Ecological Status Query Using CTEs**
    eco_query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND naturalAWBHMWB <> 'Unpopulated'
              AND surfaceWaterBodyCategory <> 'Unpopulated'
              AND surfaceWaterBodyCategory IN ({','.join('?' * len(WDFCode))})
              AND swEcologicalStatusOrPotentialValue IN ({','.join('?' * len(ecoStatus))})
        ),
        TotalCounts AS (
            SELECT countryCode, surfaceWaterBodyCategory, COUNT(*) AS total_count
            FROM FilteredData
            GROUP BY countryCode, surfaceWaterBodyCategory
        ),
        EcologicalStatusCounts AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue,
                   COUNT(*) AS num_records
            FROM FilteredData
            GROUP BY countryCode, cYear, surfaceWaterBodyCategory, swEcologicalStatusOrPotentialValue
        )
        SELECT esc.countryCode, 
               esc.cYear, 
               esc.surfaceWaterBodyCategory AS Categories, 
               esc.swEcologicalStatusOrPotentialValue AS Ecological_Status_Value,
               esc.num_records AS Number,
               ROUND(esc.num_records * 100.0 / NULLIF(tc.total_count, 0), 0) AS Number_Percent
        FROM EcologicalStatusCounts esc
        JOIN TotalCounts tc 
          ON esc.countryCode = tc.countryCode 
         AND esc.surfaceWaterBodyCategory = tc.surfaceWaterBodyCategory
        ORDER BY esc.countryCode, esc.surfaceWaterBodyCategory, esc.swEcologicalStatusOrPotentialValue;
    """

    cur.execute(eco_query, [cYear] + countryCode + WDFCode + ecoStatus)
    eco_data = cur.fetchall()


    # 🚀 **Optimized Chemical Status Query Using CTEs**
    chem_query = f"""
        WITH FilteredData AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swChemicalStatusValue
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND naturalAWBHMWB <> 'Unpopulated'
              AND surfaceWaterBodyCategory <> 'Unpopulated'
              AND swChemicalStatusValue IN ({','.join('?' * len(chemStatus))})
        ),
        TotalCounts AS (
            SELECT countryCode, surfaceWaterBodyCategory, COUNT(*) AS total_count
            FROM FilteredData
            GROUP BY countryCode, surfaceWaterBodyCategory
        ),
        ChemicalStatusCounts AS (
            SELECT countryCode, cYear, surfaceWaterBodyCategory, swChemicalStatusValue,
                   COUNT(*) AS num_records
            FROM FilteredData
            GROUP BY countryCode, cYear, surfaceWaterBodyCategory, swChemicalStatusValue
        )
        SELECT csc.countryCode, 
               csc.cYear, 
               csc.surfaceWaterBodyCategory AS Categories, 
               csc.swChemicalStatusValue AS Chemical_Status_Value,
               csc.num_records AS Number,
               ROUND(csc.num_records * 100.0 / NULLIF(tc.total_count, 0), 0) AS Number_Percent
        FROM ChemicalStatusCounts csc
        JOIN TotalCounts tc 
          ON csc.countryCode = tc.countryCode 
         AND csc.surfaceWaterBodyCategory = tc.surfaceWaterBodyCategory
        ORDER BY csc.countryCode, csc.surfaceWaterBodyCategory, csc.swChemicalStatusValue;
    """

    cur.execute(chem_query, [cYear] + countryCode + chemStatus)
    chem_data = cur.fetchall()

    # **Write Ecological Status Data to CSV**
    with open(eco_output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(eco_headers)
        writer.writerows(eco_data)

    # **Write Chemical Status Data to CSV**
    with open(chem_output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(chem_headers)
        writer.writerows(chem_data)

    conn.close()

def swb_Chemical_assessment_using_monitoring_grouping_or_expert_judgement(db_file, countryCode, cYear, working_directory):
    """
    Extracts Chemical Assessment Confidence & Monitoring results for Surface Water Bodies.
    """

    # Output file
    output_file = os.path.join(working_directory, '39.swb_Chemical_assessment_using_monitoring_grouping_or_expert_judgement2016.csv')

    # Headers
    headers = ["Country", "Year", "Chemical Assessment Confidence", "Chemical Monitoring Results", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   cYear, 
                   swChemicalAssessmentConfidence,
                   COUNT(*) AS total_per_confidence
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND swChemicalAssessmentConfidence IN ("High", "Medium", "Low", "Unknown")
            GROUP BY countryCode, cYear, swChemicalAssessmentConfidence
        )
        SELECT s.countryCode, 
               s.cYear, 
               s.swChemicalAssessmentConfidence, 
               s.swChemicalMonitoringResults, 
               COUNT(*) AS Number,
               ROUND(
                   COUNT(*) * 100.0 / NULLIF(t.total_per_confidence, 0), 0) 
               AS Percentage
        FROM SOW_SWB_SurfaceWaterBody s
        JOIN TotalCounts t 
          ON s.countryCode = t.countryCode 
         AND s.cYear = t.cYear 
         AND s.swChemicalAssessmentConfidence = t.swChemicalAssessmentConfidence
        WHERE s.cYear = ? 
          AND s.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND s.swChemicalAssessmentConfidence IN ("High", "Medium", "Low", "Unknown") 
          AND s.swChemicalMonitoringResults IN ("Missing", "Expert judgement", "Monitoring", "Grouping")
        GROUP BY s.countryCode, s.cYear, s.swChemicalAssessmentConfidence, s.swChemicalMonitoringResults;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # Write to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()    
                    
def swRBsPollutants(db_file, countryCode, cYear, working_directory):
    """
    Extracts river basin specific pollutants and computes their percentage across water bodies.
    """

    output_file = os.path.join(working_directory, '40.swRBsPollutants.csv')
    headers = ["Country", "River Basin Specific Pollutant", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query with CTE for total calculation**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_FailingRBSP
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND swFailingRBSP <> 'None'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.swFailingRBSP,
               COUNT(f.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(f.euSurfaceWaterBodyCode) * 100.0 / NULLIF(t.total_count, 0), 1
               ) AS Percentage
        FROM SOW_SWB_FailingRBSP f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.swFailingRBSP <> 'None'
        GROUP BY f.countryCode, f.swFailingRBSP;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **Formatting the output to match the requested format**
    formatted_data = []
    for row in data:
        country, pollutant, number, percentage = row
        formatted_percentage = f"{int(percentage)}%"  # Converts 43.0 to "43%"
        formatted_data.append([country, pollutant, number, formatted_percentage])

    # Write to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def swEcologicalStatusOrPotentialExpectedGoodIn2015(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Ecological Status Or Potential Expected Good In 2015 using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"44.swEcologicalStatusOrPotentialExpectedGoodIn2015.csv")
    headers = ["Country", "Ecological Status Or Potential Expected Good In 2015", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, swEcologicalStatusOrPotentialExpectedGoodIn2015
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND naturalAWBHMWB <> 'Unpopulated'
              AND swEcologicalStatusOrPotentialValue NOT IN ('inapplicable', 'Unpopulated')
              AND swEcologicalStatusOrPotentialExpectedGoodIn2015 IN ('Yes', 'No')
        ),
        TotalCounts AS (
            SELECT countryCode, COUNT(*) AS total_count
            FROM FilteredData
            GROUP BY countryCode
        ),
        StatusCounts AS (
            SELECT countryCode, swEcologicalStatusOrPotentialExpectedGoodIn2015,
                   COUNT(*) AS Number
            FROM FilteredData
            GROUP BY countryCode, swEcologicalStatusOrPotentialExpectedGoodIn2015
        )
        SELECT sc.countryCode, 
               sc.swEcologicalStatusOrPotentialExpectedGoodIn2015,
               sc.Number,
               ROUND(sc.Number * 100.0 / NULLIF(tc.total_count, 0), 0) AS Percentage
        FROM StatusCounts sc
        JOIN TotalCounts tc 
          ON sc.countryCode = tc.countryCode
        ORDER BY sc.countryCode, sc.swEcologicalStatusOrPotentialExpectedGoodIn2015;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **Write to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swEcologicalStatusOrPotentialExpectedAchievementDate(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Ecological Status Or Potential Expected Achievement Date.
    """

    output_file = os.path.join(working_directory, '45.swEcologicalStatusOrPotentialExpectedAchievementDate2016.csv')
    headers = ["Country", "Year", "Ecological Status Or Potential Expected Achievement Date", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Expected achievement date categories
    achievement_dates = [
        "Good status already achieved",
        "Less stringent objectives already achieved",
        "2016--2021",
        "2022--2027",
        "Beyond 2027",
        "Unknown"
    ]

    # 🚀 **Optimized Query with CTE for total calculation**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND naturalAWBHMWB <> 'Unpopulated' 
              AND swEcologicalStatusOrPotentialValue NOT IN ('inapplicable', 'Unpopulated') 
              AND swEcologicalStatusOrPotentialExpectedGoodIn2015 <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.swEcologicalStatusOrPotentialExpectedAchievementDate,
               COUNT(f.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(f.euSurfaceWaterBodyCode) * 100.0 / NULLIF(t.total_count, 0), 0
               ) AS Percentage
        FROM SOW_SWB_SurfaceWaterBody f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.naturalAWBHMWB <> 'Unpopulated' 
          AND f.swEcologicalStatusOrPotentialValue NOT IN ('inapplicable', 'Unpopulated') 
          AND f.swEcologicalStatusOrPotentialExpectedGoodIn2015 <> 'Unpopulated' 
          AND f.swEcologicalStatusOrPotentialExpectedAchievementDate IN ({','.join('?' * len(achievement_dates))})
        GROUP BY f.countryCode, f.cYear, f.swEcologicalStatusOrPotentialExpectedAchievementDate;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + achievement_dates)
    data = cur.fetchall()

    # **Formatting the output (Removing % Symbol)**
    formatted_data = [[country, year, date, number, percentage] for country, year, date, number, percentage in data]

    # Write to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def swChemicalStatusExpectedGoodIn2015(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Status Expected Good In 2015.
    """

    output_file = os.path.join(working_directory, '46.swChemicalStatusExpectedGoodIn2015.csv')
    headers = ["Country", "Chemical Status Expected Good In 2015", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for Chemical Status Expected Good
    expected_good_values = ["Yes", "No"]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(surfaceWaterBodyCategory) AS total_count
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND surfaceWaterBodyCategory <> 'Unpopulated' 
              AND swChemicalStatusValue <> 'Unpopulated' 
              AND swChemicalStatusExpectedGoodIn2015 <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.swChemicalStatusExpectedGoodIn2015,
               COUNT(f.surfaceWaterBodyCategory) AS Number,
               ROUND(
                   COUNT(f.surfaceWaterBodyCategory) * 100.0 / NULLIF(t.total_count, 0), 1
               ) AS Percentage
        FROM SOW_SWB_SurfaceWaterBody f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.surfaceWaterBodyCategory <> 'Unpopulated' 
          AND f.swChemicalStatusValue <> 'Unpopulated' 
          AND f.swChemicalStatusExpectedGoodIn2015 IN ({','.join('?' * len(expected_good_values))})
        GROUP BY f.countryCode, f.swChemicalStatusExpectedGoodIn2015;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + expected_good_values)
    data = cur.fetchall()

    # **Formatting the output (Removing `%` Symbol)**
    formatted_data = [[country, value, number, percentage] for country, value, number, percentage in data]

    # Write to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def swChemicalStatusExpectedAchievementDate(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Status Expected Achievement Date.
    """

    output_file = os.path.join(working_directory, '47.swChemicalStatusExpectedAchievementDate2016.csv')
    headers = ["Country", "Year", "Chemical Status Expected Achievement Date", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for Chemical Status Expected Achievement Date
    expected_achievement_dates = [
        "Good status already achieved", "Less stringent objectives already achieved",
        "2016--2021", "2022--2027", "Beyond 2027", "Unknown"
    ]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(swChemicalStatusExpectedAchievementDate) AS total_count
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND swChemicalStatusExpectedAchievementDate <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.swChemicalStatusExpectedAchievementDate,
               COUNT(f.swChemicalStatusExpectedAchievementDate) AS Number,
               ROUND(
                   COUNT(f.swChemicalStatusExpectedAchievementDate) * 100.0 / NULLIF(t.total_count, 0), 0
               ) AS Percentage
        FROM SOW_SWB_SurfaceWaterBody f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.swChemicalStatusExpectedAchievementDate <> 'Unpopulated'
          AND f.swChemicalStatusExpectedAchievementDate IN ({','.join('?' * len(expected_achievement_dates))})
        GROUP BY f.countryCode, f.cYear, f.swChemicalStatusExpectedAchievementDate;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + expected_achievement_dates)
    data = cur.fetchall()

    # **Formatting the output (Removing `%` Symbol)**
    formatted_data = [[country, year, date, number, percentage] for country, year, date, number, percentage in data]

    # Write to CSV
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def GroundWaterBodyCategory2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts Ground Water Body Categories using CTEs and manual median calculations.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"2.GroundWaterBodyCategory{cYear}.csv")
    headers = ["Country", "Year", "Number", "Number(%)", "Area", "Area(%)", "Median Area (km^2)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, groundWaterBodyName, cArea
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
        ),
        TotalCounts AS (
            SELECT COUNT(groundWaterBodyName) AS total_number,
                   SUM(cArea) AS total_area
            FROM FilteredData
        ),
        MedianArea AS (
            SELECT countryCode, AVG(cArea) AS median_area
            FROM (
                SELECT countryCode, cArea, 
                       ROW_NUMBER() OVER (PARTITION BY countryCode ORDER BY cArea) AS row_num,
                       COUNT(*) OVER (PARTITION BY countryCode) AS total_count
                FROM FilteredData
                WHERE cArea IS NOT NULL
            )
            WHERE row_num = (total_count + 1) / 2 OR row_num = (total_count / 2) + 1
            GROUP BY countryCode
        ),
        GroundwaterStats AS (
            SELECT countryCode, 
                   ? AS cYear,
                   COUNT(groundWaterBodyName) AS Number,
                   ROUND(COUNT(groundWaterBodyName) * 100.0 / NULLIF(tc.total_number, 0), 0) AS Number_Percent,
                   ROUND(SUM(cArea), 0) AS Area,
                   ROUND(SUM(cArea) * 100.0 / NULLIF(tc.total_area, 0), 0) AS Area_Percent
            FROM FilteredData
            JOIN TotalCounts tc ON 1=1
            GROUP BY countryCode
        )
        SELECT gs.*, 
               ROUND(COALESCE(mA.median_area, 0), 0) AS Median_Area
        FROM GroundwaterStats gs
        LEFT JOIN MedianArea mA ON gs.countryCode = mA.countryCode
        ORDER BY gs.countryCode;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + [cYear])
    data = cur.fetchall()

    # **Write to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Groundwater_bodies_Chemical_Exemption_Type(db_file, countryCode, cYear, working_directory):
    """
    Extracts Groundwater Chemical Exemption Types using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"7.Groundwater_bodies_Chemical_Exemption_Type{cYear}.csv")
    headers = ["Country", "Chemical Exemption Type Group", "Chemical Exemption Type", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Optimized Query Using CTEs
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, 
                   gwChemicalExemptionTypeGroup, 
                   gwChemicalExemptionType, 
                   cArea
            FROM SOW_GWB_GWP_GWChemicalExemptionType
            WHERE cYear = ?
              AND gwChemicalStatusValue NOT IN ('2', 'missing', 'unpopulated')
              AND countryCode IN ({','.join('?' * len(countryCode))})
        ),
        TotalArea AS (
            SELECT countryCode, 
                   ROUND(SUM(DISTINCT cArea), 0) AS total_area
            FROM FilteredData
            GROUP BY countryCode
        )
        SELECT fd.countryCode, 
               COALESCE(fd.gwChemicalExemptionTypeGroup, 'None') AS ChemicalExemptionGroup,
               COALESCE(fd.gwChemicalExemptionType, 'None') AS ChemicalExemptionType,
               ta.total_area,
               100.0 AS Area_Percent  
        FROM FilteredData fd
        JOIN TotalArea ta 
          ON fd.countryCode = ta.countryCode
        GROUP BY fd.countryCode, fd.gwChemicalExemptionTypeGroup, fd.gwChemicalExemptionType, ta.total_area
        ORDER BY fd.countryCode, fd.gwChemicalExemptionTypeGroup, fd.gwChemicalExemptionType;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **Write to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Groundwater_bodies_Quantitative_Exemption_Type(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Bodies Quantitative Exemption Type.
    """

    output_file = os.path.join(working_directory, '7.Groundwater_bodies_Quantitative_Exemption_Type2016.csv')
    headers = ["Country", "Quantitative Exemption Type Group", "Quantitative Exemption Type",
               "Quantitative Exemption Pressure Group", "Quantitative Exemption Pressure", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    cur.execute(f"""
        SELECT DISTINCT gwQuantitativeExemptionTypeGroup, gwQuantitativeExemptionType,
                        gwQuantitativeExemptionPressureGroup, gwQuantitativeExemptionPressure
        FROM SOW_GWB_gwQuantitativeExemptionPressure
        WHERE cYear = ? AND gwQuantitativeExemptionTypeGroup <> "None" AND countryCode IN ({','.join('?' * len(countryCode))})
    """, [cYear] + countryCode)
    distinct_values = cur.fetchall()

    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, SUM(cArea) AS total_area
            FROM SOW_GWB_gwQuantitativeExemptionPressure
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
            GROUP BY countryCode
        )
        SELECT f.countryCode,
               f.gwQuantitativeExemptionTypeGroup,
               f.gwQuantitativeExemptionType,
               f.gwQuantitativeExemptionPressureGroup,
               f.gwQuantitativeExemptionPressure,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Percentage
        FROM SOW_GWB_gwQuantitativeExemptionPressure f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))})
          AND f.gwQuantitativeExemptionTypeGroup IN ({','.join('?' * len(set(row[0] for row in distinct_values)))})
          AND f.gwQuantitativeExemptionType IN ({','.join('?' * len(set(row[1] for row in distinct_values)))})
          AND f.gwQuantitativeExemptionPressureGroup IN ({','.join('?' * len(set(row[2] for row in distinct_values)))})
          AND f.gwQuantitativeExemptionPressure IN ({','.join('?' * len(set(row[3] for row in distinct_values)))})
        GROUP BY f.countryCode, f.gwQuantitativeExemptionTypeGroup, f.gwQuantitativeExemptionType,
                 f.gwQuantitativeExemptionPressureGroup, f.gwQuantitativeExemptionPressure;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode +
                list(set(row[0] for row in distinct_values)) +
                list(set(row[1] for row in distinct_values)) +
                list(set(row[2] for row in distinct_values)) +
                list(set(row[3] for row in distinct_values)))
    data = cur.fetchall()

    formatted_data = [[country, type_group, type_name, pressure_group, pressure, area, percent]
                      for country, type_group, type_name, pressure_group, pressure, area, percent in data]

    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def gwChemical_exemptions_and_pressures(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Chemical Exemptions and Pressures.
    """

    output_file = os.path.join(working_directory, '7.gwChemical_exemptions_and_pressures.csv')
    headers = ["Country", "Chemical Exemption Type Group", "Chemical Exemption Type",
               "Chemical Pressure Type Group", "Chemical Pressure Type", "Area (km^2)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH DistinctValues AS (
            SELECT DISTINCT countryCode, euGroundWaterBodyCode, cArea,
                   COALESCE(gwChemicalExemptionTypeGroup, 'None') AS gwChemicalExemptionTypeGroup,
                   COALESCE(gwChemicalExemptionType, 'None') AS gwChemicalExemptionType,
                   COALESCE(gwChemicalExemptionPressureGroup, 'Inapplicable') AS gwChemicalExemptionPressureGroup,
                   COALESCE(gwChemicalExemptionPressure, 'Inapplicable') AS gwChemicalExemptionPressure
            FROM SOW_GWB_GWP_GWC_gwChemicalExemptionPressure
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
        )
        SELECT countryCode,
               gwChemicalExemptionTypeGroup,
               gwChemicalExemptionType,
               gwChemicalExemptionPressureGroup,
               gwChemicalExemptionPressure,
               ROUND(SUM(cArea), 0) AS Area_km2
        FROM DistinctValues
        GROUP BY countryCode, gwChemicalExemptionTypeGroup, gwChemicalExemptionType,
                 gwChemicalExemptionPressureGroup, gwChemicalExemptionPressure;
    """

    # 🚀 **Execute Query with Parameters**
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # 🚀 **Format Data & Write to CSV**
    formatted_data = [[country, type_group, type_name, pressure_group, pressure, area]
                      for country, type_group, type_name, pressure_group, pressure, area in data]

    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def Groundwater_bodies_Quantitative_exemptions_and_pressures(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Bodies Quantitative Exemptions and Pressures.
    """

    output_file = os.path.join(working_directory, '7.Groundwater_bodies_Quantitative_exemptions_and_pressures2016.csv')
    headers = ["Country", "Year", "Quantitative Exemption Type Group", "Quantitative Exemption Type",
               "Quantitative Exemption Pressure Group", "Quantitative Exemption Pressure", "Area (km^2)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH DistinctValues AS (
            SELECT DISTINCT countryCode, cYear, euGroundWaterBodyCode, cArea,
                   COALESCE(gwQuantitativeExemptionTypeGroup, 'None') AS gwQuantitativeExemptionTypeGroup,
                   COALESCE(gwQuantitativeExemptionType, 'None') AS gwQuantitativeExemptionType,
                   COALESCE(gwQuantitativeExemptionPressureGroup, 'Inapplicable') AS gwQuantitativeExemptionPressureGroup,
                   COALESCE(gwQuantitativeExemptionPressure, 'Inapplicable') AS gwQuantitativeExemptionPressure
            FROM SOW_GWB_gwQuantitativeExemptionPressure
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
        )
        SELECT countryCode,
               cYear,
               gwQuantitativeExemptionTypeGroup,
               gwQuantitativeExemptionType,
               gwQuantitativeExemptionPressureGroup,
               gwQuantitativeExemptionPressure,
               ROUND(SUM(cArea), 0) AS Area_km2
        FROM DistinctValues
        GROUP BY countryCode, cYear, gwQuantitativeExemptionTypeGroup, gwQuantitativeExemptionType,
                 gwQuantitativeExemptionPressureGroup, gwQuantitativeExemptionPressure;
    """

    # 🚀 **Execute Query with Parameters**
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # 🚀 **Format Data & Write to CSV**
    formatted_data = [[country, year, type_group, type_name, pressure_group, pressure, area]
                      for country, year, type_group, type_name, pressure_group, pressure, area in data]

    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def SOW_GWB_GroundWaterBody_GWB_Chemical_status(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Body Chemical Status.
    """

    output_file = os.path.join(working_directory, '20.GroundWaterBodyCategoryChemical_status2016.csv')
    headers = ["Country", "Year", "Chemical Status Value", "Area (km^2)", "Area (%)", "Number"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwChemicalStatusValue,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percentage,
               COUNT(DISTINCT f.euGroundWaterBodyCode) AS Number
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwChemicalStatusValue IN ("2", "3", "unknown")
        GROUP BY f.countryCode, f.cYear, f.gwChemicalStatusValue;
    """

    # 🚀 **Execute Query with Parameters**
    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # 🚀 **Format Data & Write to CSV**
    formatted_data = [[country, year, status, area, percentage, number]
                      for country, year, status, area, percentage, number in data]

    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def SOW_GWB_GroundWaterBody_GWB_Quantitative_status(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Body Quantitative Status.
    """

    output_file = os.path.join(working_directory, '18.GroundWaterBodyCategoryQuantitative_status2016.csv')
    headers = ["Country", "Year", "Quantitative Status Value", "Area (km^2)", "Number"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        SELECT countryCode, 
               cYear, 
               gwQuantitativeStatusValue, 
               ROUND(SUM(cArea), 0) AS Area_km2,
               COUNT(DISTINCT euGroundWaterBodyCode) AS Number
        FROM SOW_GWB_GroundWaterBody
        WHERE cYear = ? 
          AND countryCode IN ({','.join('?' * len(countryCode))}) 
          AND gwQuantitativeStatusValue IN ("2", "3", "unknown")
        GROUP BY countryCode, cYear, gwQuantitativeStatusValue;
    """

    # 🚀 **Execute Query with Parameters**
    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # 🚀 **Format Data & Write to CSV**
    formatted_data = [[country, year, status, area, number]
                      for country, year, status, area, number in data]

    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()
               
def gwQuantitativeStatusValue_gwChemicalStatusValue(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Quantitative and Chemical Status Values by Country using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **🚀 Optimized Query for Quantitative Status**
    quantitative_query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
            GROUP BY countryCode
        ),
        QuantitativeStatus AS (
            SELECT f.countryCode, 
                   f.cYear,
                   f.gwQuantitativeStatusValue,
                   ROUND(SUM(f.cArea), 0) AS Area_km2
            FROM SOW_GWB_GroundWaterBody f
            WHERE f.cYear = ? 
              AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
              AND f.gwQuantitativeStatusValue IN ("2", "3", "unknown")
            GROUP BY f.countryCode, f.cYear, f.gwQuantitativeStatusValue
        )
        SELECT qs.countryCode, 
               qs.cYear,
               qs.gwQuantitativeStatusValue,
               qs.Area_km2,
               ROUND(qs.Area_km2 * 100.0 / NULLIF(ta.total_area, 0), 0) AS Area_Percent
        FROM QuantitativeStatus qs
        JOIN TotalArea ta 
          ON qs.countryCode = ta.countryCode
        ORDER BY qs.countryCode, qs.gwQuantitativeStatusValue;
    """

    cur.execute(quantitative_query, [cYear] + countryCode + [cYear] + countryCode)
    quantitative_data = cur.fetchall()

    # **🚀 Optimized Query for Chemical Status Percentage**
    chemical_query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
            GROUP BY countryCode
        ),
        ChemicalStatus AS (
            SELECT f.countryCode, 
                   f.cYear,
                   f.gwChemicalStatusValue,
                   ROUND(SUM(f.cArea), 0) AS Area_km2
            FROM SOW_GWB_GroundWaterBody f
            WHERE f.cYear = ? 
              AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
              AND f.gwChemicalStatusValue IN ("2", "3")
            GROUP BY f.countryCode, f.cYear, f.gwChemicalStatusValue
        )
        SELECT cs.countryCode, 
               cs.cYear AS Year,
               cs.gwChemicalStatusValue AS "Chemical Status Value",
               cs.Area_km2 AS "Area (km^2)",
               ROUND(cs.Area_km2 * 100.0 / NULLIF(ta.total_area, 0), 0) AS "Area (%)"
        FROM ChemicalStatus cs
        JOIN TotalArea ta 
          ON cs.countryCode = ta.countryCode
        ORDER BY cs.countryCode, cs.gwChemicalStatusValue;
    """

    # Execute query
    cur.execute(chemical_query, [cYear] + countryCode + [cYear] + countryCode)
    chemical_data = cur.fetchall()

    # **📌 Write Quantitative Status Data to CSV**
    quantitative_output_file = os.path.join(working_directory, f"22.gwQuantitativeStatusValue_Percent_Country_{cYear}.csv")
    with open(quantitative_output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Country", "Year", "Quantitative Status Value", "Area (km^2)", "Area (%)"])
        writer.writerows(quantitative_data)

    # **📌 Write Chemical Status Percentage Data to CSV**
    chemical_output_file = os.path.join(working_directory, f"22.gwChemicalStatusValue_Percent_Country_{cYear}.csv")
    with open(chemical_output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Country", "Year", "Chemical Status Value", "Area (km^2)","Area (%)"])
        writer.writerows(chemical_data)

    conn.close()

def Groundwater_bodies_At_risk_of_failing_to_achieve_good_quantitative_status(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater bodies at risk of failing to achieve good quantitative status.
    """

    output_file = os.path.join(working_directory, '25.Groundwater_bodies_At_risk_of_failing_to_achieve_good_quantitative_status2016.csv')
    headers = ["Country", "Year", "Quantitative Status Value", "Area (km^2)", "Area (%)", "Number"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for At-Risk Quantitative Status
    gwAtRiskQuantitative = ["Yes", "No"]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwEORiskQuantitative NOT IN ('Not in WFD2010', 'None', 'Unpopulated')
              AND gwAtRiskQuantitative <> 'Unpopulated'
              AND gwQuantitativeStatusValue <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwAtRiskQuantitative,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent,
               COUNT(DISTINCT f.euGroundWaterBodyCode) AS Number
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwAtRiskQuantitative IN ({','.join('?' * len(gwAtRiskQuantitative))})
          AND f.gwEORiskQuantitative NOT IN ('Not in WFD2010', 'None', 'Unpopulated')
          AND f.gwAtRiskQuantitative <> 'Unpopulated'
          AND f.gwQuantitativeStatusValue <> 'Unpopulated'
        GROUP BY f.countryCode, f.cYear, f.gwAtRiskQuantitative;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + gwAtRiskQuantitative)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def SOW_GWB_gwQuantitativeReasonsForFailure_Table(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Quantitative Reasons For Failure.
    """

    output_file = os.path.join(working_directory, '25.SOW_GWB_gwQuantitativeReasonsForFailure_Table2016.csv')
    headers = ["Country", "Year", "Quantitative Status Value", "Quantitative Reasons For Failure", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible reasons for failure
    reasonOfFailure = [
        "Good status already achieved", "Water balance / Lowering water table",
        "Saline or other intrusion", "Dependent terrestrial ecosystems",
        "Associated surface waters"
    ]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwAssociatedProtectedArea <> 'Unpopulated' 
              AND gwQuantitativeStatusValue = '3'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwQuantitativeStatusValue,
               f.gwQuantitativeReasonsForFailure,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_gwQuantitativeReasonsForFailure f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwQuantitativeReasonsForFailure <> 'Unpopulated'
          AND f.gwQuantitativeStatusValue = '3'
          AND f.gwQuantitativeReasonsForFailure IN ({','.join('?' * len(reasonOfFailure))})
        GROUP BY f.countryCode, f.cYear, f.gwQuantitativeStatusValue, f.gwQuantitativeReasonsForFailure;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + reasonOfFailure)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def SOW_GWB_gwChemicalReasonsForFailure_Table(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Reasons for Failure.
    """

    output_file = os.path.join(working_directory, '26.gwChemicalReasonsForFailure_Table2016.csv')
    headers = ["Country", "Year", "Chemical Status Value", "Chemical Reasons For Failure", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_gwChemicalReasonsForFailure
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwAtRiskChemical NOT IN ('Annex 0', 'Unpopulated') 
              AND gwChemicalStatusValue = '3'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwChemicalStatusValue,
               f.gwChemicalReasonsForFailure,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_gwChemicalReasonsForFailure f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwAtRiskChemical NOT IN ('Annex 0', 'Unpopulated')
          AND f.gwChemicalStatusValue = '3'
        GROUP BY f.countryCode, f.cYear, f.gwChemicalStatusValue, f.gwChemicalReasonsForFailure;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()
                
def gwChemicalStatusValue_Table(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Status Value.
    """

    output_file = os.path.join(working_directory, '26.gwChemicalStatusValue_Table2016.csv')
    headers = ["Country", "Year", "Chemical Status Value", "Area (km^2)", "Area (%)", "Number"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwEORiskChemical NOT IN ('Unpopulated', 'Not in WFD2010')
              AND gwAtRiskChemical NOT IN ('Unpopulated', 'Not in WFD2010')
              AND gwChemicalStatusValue <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwAtRiskChemical,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent,
               COUNT(DISTINCT f.euGroundWaterBodyCode) AS Number
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwEORiskChemical NOT IN ('Unpopulated', 'Not in WFD2010')
          AND f.gwAtRiskChemical NOT IN ('Unpopulated', 'Not in WFD2010')
          AND f.gwChemicalStatusValue <> 'Unpopulated'
          AND f.gwAtRiskChemical IN ('No', 'Yes')
        GROUP BY f.countryCode, f.cYear, f.gwAtRiskChemical;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwQuantitativeStatusExpectedGoodIn2015(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Quantitative Status Expected Good In 2015.
    """

    output_file = os.path.join(working_directory, '29.gwQuantitativeStatusExpectedGoodIn2015.csv')
    headers = ["Country", "Year", "Quantitative Status Expected Good In 2015", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for `gwQuantitativeStatusExpectedGoodIn2015`
    expected_good_status = ["Yes", "No"]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwQuantitativeStatusValue <> 'Unpopulated'
              AND gwQuantitativeStatusExpectedGoodIn2015 <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwQuantitativeStatusExpectedGoodIn2015,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwQuantitativeStatusExpectedGoodIn2015 IN ({','.join('?' * len(expected_good_status))})
        GROUP BY f.countryCode, f.cYear, f.gwQuantitativeStatusExpectedGoodIn2015;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + expected_good_status)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwQuantitativeStatusExpectedAchievementDate(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Quantitative Status Expected Achievement Date.
    """

    output_file = os.path.join(working_directory, '30.gwQuantitativeStatusExpectedAchievementDate2016.csv')
    headers = ["Country", "Year", "Quantitative Status Expected Date", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for `gwQuantitativeStatusExpectedAchievementDate`
    expected_achievement_dates = [
        "Good status already achieved", "Less stringent objectives already achieved",
        "2016--2021", "2022--2027", "Beyond 2027", "Unknown"
    ]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwQuantitativeStatusExpectedAchievementDate <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwQuantitativeStatusExpectedAchievementDate,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwQuantitativeStatusExpectedAchievementDate IN ({','.join('?' * len(expected_achievement_dates))})
        GROUP BY f.countryCode, f.cYear, f.gwQuantitativeStatusExpectedAchievementDate;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + expected_achievement_dates)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwChemicalStatusExpectedGoodIn2015(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Status Expected Good in 2015.
    """

    output_file = os.path.join(working_directory, '31.gwChemicalStatusExpectedGoodIn2015.csv')
    headers = ["Country", "Year", "Chemical Status Expected Achievement Date", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for `gwChemicalStatusExpectedGoodIn2015`
    expected_good_status = ["Yes", "No"]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwChemicalStatusValue <> 'Unpopulated'
              AND gwChemicalStatusExpectedGoodIn2015 <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwChemicalStatusExpectedGoodIn2015,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwChemicalStatusExpectedGoodIn2015 IN ({','.join('?' * len(expected_good_status))})
        GROUP BY f.countryCode, f.cYear, f.gwChemicalStatusExpectedGoodIn2015;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + expected_good_status)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwChemicalStatusExpectedAchievementDate(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Status Expected Achievement Date.
    """

    output_file = os.path.join(working_directory, '32.gwChemicalStatusExpectedAchievementDate2016.csv')
    headers = ["Country", "Year", "Chemical Status Expected Achievement Date", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for `gwChemicalStatusExpectedAchievementDate`
    expected_achievement_dates = [
        "Good status already achieved", "Less stringent objectives already achieved",
        "2016--2021", "2022--2027", "Beyond 2027", "Unknown"
    ]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwChemicalStatusExpectedAchievementDate <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwChemicalStatusExpectedAchievementDate,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwChemicalStatusExpectedAchievementDate IN ({','.join('?' * len(expected_achievement_dates))})
        GROUP BY f.countryCode, f.cYear, f.gwChemicalStatusExpectedAchievementDate;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + expected_achievement_dates)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwQuantitativeAssessmentConfidence(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Quantitative Assessment Confidence.
    """

    output_file = os.path.join(working_directory, '35.gwQuantitativeAssessmentConfidence2016.csv')
    headers = ["Country", "Year", "Quantitative Assessment Confidence", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for `gwQuantitativeAssessmentConfidence`
    confidence_levels = ["High", "Medium", "Low", "Unknown"]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwQuantitativeAssessmentConfidence <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwQuantitativeAssessmentConfidence,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwQuantitativeAssessmentConfidence IN ({','.join('?' * len(confidence_levels))})
        GROUP BY f.countryCode, f.cYear, f.gwQuantitativeAssessmentConfidence;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + confidence_levels)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwChemicalAssessmentConfidence(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Chemical Assessment Confidence.
    """

    output_file = os.path.join(working_directory, '36.gwChemicalAssessmentConfidence2016.csv')
    headers = ["Country", "Year", "Chemical Assessment Confidence", "Area (km^2)", "Area(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Possible values for `gwChemicalAssessmentConfidence`
    confidence_levels = ["High", "Medium", "Low", "Unknown"]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwChemicalAssessmentConfidence <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwChemicalAssessmentConfidence,
               ROUND(SUM(f.cArea), 0) AS Area_km2,
               ROUND(SUM(f.cArea) * 100.0 / NULLIF(t.total_area, 0), 0) AS Area_Percent
        FROM SOW_GWB_GroundWaterBody f
        JOIN TotalArea t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwChemicalAssessmentConfidence IN ({','.join('?' * len(confidence_levels))})
        GROUP BY f.countryCode, f.cYear, f.gwChemicalAssessmentConfidence;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + confidence_levels)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def Number_of_groundwater_bodies_failing_to_achieve_good_status(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for groundwater bodies failing to achieve good status.
    """

    output_file = os.path.join(working_directory, '37.Number_of_groundwater_bodies_failing_to_achieve_good_status.csv')
    headers = ["Country", "Year", "Good", "Failing", "Number"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH StatusCounts AS (
            SELECT countryCode, 
                   cYear,
                   COUNT(DISTINCT CASE WHEN gwChemicalStatusValue = '2' AND gwQuantitativeStatusValue = '2' THEN euGroundWaterBodyCode END) AS Good,
                   COUNT(DISTINCT CASE WHEN gwChemicalStatusValue = '3' OR gwQuantitativeStatusValue = '3' THEN euGroundWaterBodyCode END) AS Failing,
                   COUNT(DISTINCT euGroundWaterBodyCode) AS Total
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
            GROUP BY countryCode, cYear
        )
        SELECT countryCode, cYear, Good, Failing, Total
        FROM StatusCounts;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def geologicalFormation(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for groundwater geological formations.
    """

    output_file = os.path.join(working_directory, '38.GWB_geologicalFormation2016.csv')
    headers = ["Country", "Year", "Geological Formation", "Area (km^2)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    geological_formations = [
        "Porous aquifers - highly productive",
        "Porous aquifers - moderately productive",
        "Fissured aquifers including karst - highly productive",
        "Fissured aquifers including karst - moderately productive",
        "Fractured aquifers - highly productive",
        "Fractured aquifers - moderately productive"
    ]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH FilteredData AS (
            SELECT countryCode, 
                   cYear, 
                   geologicalFormation, 
                   SUM(cArea) AS total_area
            FROM SOW_GWB_GroundWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND geologicalFormation NOT IN ('Missing', 'Unknown', 'Insignificant aquifers - local and limited groundwater', 'unpopulated')
              AND gwQuantitativeStatusValue <> 'unknown'
            GROUP BY countryCode, cYear, geologicalFormation
        )
        SELECT countryCode, cYear, geologicalFormation, ROUND(total_area, 0) 
        FROM FilteredData
        WHERE geologicalFormation IN ({','.join('?' * len(geological_formations))});
    """

    cur.execute(query, [cYear] + countryCode + geological_formations)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swNumber_of_Impacts_by_country(db_file, countryCode, cYear, working_directory):
    """
    Extracts the number of significant impact types for surface water bodies by country.
    """

    output_file = os.path.join(working_directory, 'NewDash.7.swNumber_of_impacts_by_country_2016.csv')
    headers = [
        'Country', 'Year',
        'Impact 0 - Number', 'Impact 0 - Number (%)',
        'Impact 1 - Number', 'Impact 1 - Number (%)',
        'Impact 2 - Number', 'Impact 2 - Number (%)',
        'Impact 3 - Number', 'Impact 3 - Number (%)',
        'Impact 4+ - Number', 'Impact 4+ - Number (%)'
    ]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH ImpactCounts AS (
            SELECT countryCode, 
                   cYear,
                   euSurfaceWaterBodyCode,
                   COUNT(CASE WHEN swSignificantImpactType <> 'None' THEN swSignificantImpactType END) AS impact_count
            FROM SOW_SWB_SWB_swSignificantImpactType
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
            GROUP BY countryCode, cYear, euSurfaceWaterBodyCode
        ),
        CategorizedImpacts AS (
            SELECT countryCode, 
                   cYear,
                   COUNT(CASE WHEN impact_count = 0 THEN 1 END) AS Impact_0,
                   COUNT(CASE WHEN impact_count = 1 THEN 1 END) AS Impact_1,
                   COUNT(CASE WHEN impact_count = 2 THEN 1 END) AS Impact_2,
                   COUNT(CASE WHEN impact_count = 3 THEN 1 END) AS Impact_3,
                   COUNT(CASE WHEN impact_count >= 4 THEN 1 END) AS Impact_4plus,
                   COUNT(*) AS Total
            FROM ImpactCounts
            GROUP BY countryCode, cYear
        )
        SELECT countryCode, 
               cYear,
               Impact_0,
               ROUND(Impact_0 * 100.0 / NULLIF(Total, 0), 0),
               Impact_1,
               ROUND(Impact_1 * 100.0 / NULLIF(Total, 0), 0),
               Impact_2,
               ROUND(Impact_2 * 100.0 / NULLIF(Total, 0), 0),
               Impact_3,
               ROUND(Impact_3 * 100.0 / NULLIF(Total, 0), 0),
               Impact_4plus,
               ROUND(Impact_4plus * 100.0 / NULLIF(Total, 0), 0)
        FROM CategorizedImpacts;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swSignificant_Pressure_Type_Table2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts the significant pressure types for surface water bodies.
    """

    output_file = os.path.join(working_directory, '4.swSignificant_Pressure_Type_Table2016.csv')
    headers = ['Country', 'Significant Pressure Type Group', 'Significant Pressure Type', 'Number', 'Number(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # Define all relevant pressure types
    swSignificantPressureTypeGroup = [
        "P1 - Point sources", "P2 - Diffuse sources", "P2-7 - Diffuse - Atmospheric deposition ",
        "P3 - Abstraction", "P4 - Hydromorphology", "P5 - Introduced species and litter",
        "P6 - Groundwater recharge or water level", "P7 - Anthropogenic pressure - Other",
        "P8 - Anthropogenic pressure - Unknown", "P9 - Anthropogenic pressure - Historical pollution",
        "P0 - No significant anthropogenic pressure"
    ]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   swSignificantPressureTypeGroup, 
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_SWB_swSignificantPressureType
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swSignificantPressureType <> 'Unpopulated'
              AND naturalAWBHMWB <> 'Unpopulated'
              AND surfaceWaterBodyCategory <> 'Unpopulated'
              AND swEcologicalStatusOrPotentialValue <> 'Unpopulated'
              AND swEcologicalStatusOrPotentialValue <> 'inapplicable'
              AND swChemicalStatusValue <> 'Unpopulated'
            GROUP BY countryCode, swSignificantPressureTypeGroup
        )
        SELECT f.countryCode,
               f.swSignificantPressureTypeGroup,
               f.swSignificantPressureType,
               COUNT(DISTINCT f.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(DISTINCT f.euSurfaceWaterBodyCode) * 100.0 / NULLIF(t.total_count, 0), 0
               ) AS Percentage
        FROM SOW_SWB_SWB_swSignificantPressureType f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode 
         AND f.swSignificantPressureTypeGroup = t.swSignificantPressureTypeGroup
        WHERE f.cYear = ?
          AND f.countryCode IN ({','.join('?' * len(countryCode))})
          AND f.swSignificantPressureType <> 'Unpopulated'
          AND f.naturalAWBHMWB <> 'Unpopulated'
          AND f.surfaceWaterBodyCategory <> 'Unpopulated'
          AND f.swEcologicalStatusOrPotentialValue <> 'Unpopulated'
          AND f.swEcologicalStatusOrPotentialValue <> 'inapplicable'
          AND f.swChemicalStatusValue <> 'Unpopulated'
          AND f.swSignificantPressureTypeGroup IN ({','.join('?' * len(swSignificantPressureTypeGroup))})
        GROUP BY f.countryCode, f.swSignificantPressureTypeGroup, f.swSignificantPressureType;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + swSignificantPressureTypeGroup)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def SignificantImpactType_Table2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts significant impact types for surface water bodies.
    """

    output_file = os.path.join(working_directory, '4.SignificantImpactType_Table2016.csv')
    headers = ['Country', 'Significant Impact Type', 'Number', 'Number(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(DISTINCT euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_SWB_swSignificantImpactType
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND surfaceWaterBodyCategory <> 'Unpopulated'
              AND swSignificantImpactType <> 'Unpopulated'
              AND swEcologicalStatusOrPotentialValue <> 'Unpopulated'
              AND swChemicalStatusValue <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.swSignificantImpactType,
               COUNT(DISTINCT f.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(DISTINCT f.euSurfaceWaterBodyCode) * 100.0 / NULLIF(t.total_count, 0), 0
               ) AS Percentage
        FROM SOW_SWB_SWB_swSignificantImpactType f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ?
          AND f.countryCode IN ({','.join('?' * len(countryCode))})
          AND f.surfaceWaterBodyCategory <> 'Unpopulated'
          AND f.swSignificantImpactType <> 'Unpopulated'
          AND f.swEcologicalStatusOrPotentialValue <> 'Unpopulated'
          AND f.swChemicalStatusValue <> 'Unpopulated'
        GROUP BY f.countryCode, f.swSignificantImpactType;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swSignificantImpactType_Table_Other2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts significant impact 'Other' types for surface water bodies.
    """

    output_file = os.path.join(working_directory, '4.swSignificantImpactType_Table_Other2016.csv')
    headers = ['Country', 'Significant Impact Other', 'Number', 'Number(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(swSignificantImpactOther) AS total_count
            FROM SOW_SWB_swSignificantImpactOther
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND surfaceWaterBodyCategory <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.swSignificantImpactOther,
               COUNT(f.swSignificantImpactOther) AS Number,
               ROUND(
                   COUNT(f.swSignificantImpactOther) * 100.0 / NULLIF(t.total_count, 0), 1
               ) AS Percentage
        FROM SOW_SWB_swSignificantImpactOther f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ?
          AND f.countryCode IN ({','.join('?' * len(countryCode))})
          AND f.surfaceWaterBodyCategory <> 'Unpopulated'
          AND f.swSignificantImpactOther NOT IN (
              "Adverse effects on ecological indices",
              "Dėl nežinomų priežasčių geros ekologinės būklės reikalavimų neatitinka biologinių elementų rodikliai",
              "Dėl nežinomų priežasčių geros ekologinės būklės reikalavimų galimai neatitinka biologiniai rodikliai (yra netikrumas dėl būklės)",
              "Dėl galimo žuvininkystės tvenkinių poveikio geros ekologinės būklės reikalavimų neatitinka biologinių elementų rodikliai",
              "Dėl istorinės taršos geros ekologinės būklės reikalavimų neatitinka biologinių elementų rodikliai",
              "Dėl galimo žuvininkystės taršos poveikio geros ekologinės būklės reikalavimų neatitinka biologinių elementų rodikliai",
              "Dėl bendro istorinės ir dabartinės taršos poveikio geros ekologinės būklės reikalavimų neatitinka biologinių elementų rodikliai",
              "Nustatytas specifinių teršalų koncentracijų viršijimas",
              "Urban run-off"
          )
        GROUP BY f.countryCode, f.swSignificantImpactOther
        ORDER BY Number DESC;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()            

def swSignificantPressureType_Table_Other(db_file, countryCode, cYear, working_directory):
    """
    Extracts significant pressure 'Other' types for surface water bodies.
    """

    output_file = os.path.join(working_directory, '4.swSignificantPressureType_Table_Other.csv')
    headers = ['Country', 'Significant Pressure Other', 'Number', 'Number(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(swSignificantPressureOther) AS total_count
            FROM SOW_SWB_swSignificantPressureOther
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.swSignificantPressureOther,
               COUNT(f.swSignificantPressureOther) AS Number,
               ROUND(
                   COUNT(f.swSignificantPressureOther) * 100.0 / NULLIF(t.total_count, 0), 1
               ) AS Percentage
        FROM SOW_SWB_swSignificantPressureOther f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ?
          AND f.countryCode IN ({','.join('?' * len(countryCode))})
        GROUP BY f.countryCode, f.swSignificantPressureOther
        ORDER BY Number DESC;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV (UTF-8 for special characters)**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwSignificantImpactTypeByCountry(db_file, countryCode, cYear, working_directory):
    """
    Extracts significant groundwater impact types by country.
    """

    output_file = os.path.join(working_directory, '5.1.gwSignificantImpactTypeByCountry.csv')
    headers = ['Country', 'Impact 0 - Area (km^2)', 'Impact 0 - Area (%)',
               'Impact 1 - Area (km^2)', 'Impact 1 - Area (%)',
               'Impact 2 - Area (km^2)', 'Impact 2 - Area (%)',
               'Impact 3 - Area (km^2)', 'Impact 3 - Area (%)',
               'Impact 4+ - Area (km^2)', 'Impact 4+ - Area (%)',
               'No', 'Inapplicable', 'Unknown']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH ImpactCounts AS (
            SELECT countryCode,
                   cYear,
                   euGroundWaterBodyCode,
                   cArea,
                   CASE WHEN gwSignificantImpactType = "None" THEN 1 ELSE 0 END AS Impact_0,
                   CASE WHEN gwSignificantImpactType IS NOT NULL THEN 1 ELSE 0 END AS Impact_Count
            FROM SOW_GWB_gwSignificantImpactType
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
        ),
        AggregatedImpacts AS (
            SELECT countryCode,
                   SUM(CASE WHEN Impact_0 = 1 THEN cArea ELSE 0 END) AS Impact_0_Area,
                   SUM(CASE WHEN Impact_Count = 1 THEN cArea ELSE 0 END) AS Impact_1_Area,
                   SUM(CASE WHEN Impact_Count = 2 THEN cArea ELSE 0 END) AS Impact_2_Area,
                   SUM(CASE WHEN Impact_Count = 3 THEN cArea ELSE 0 END) AS Impact_3_Area,
                   SUM(CASE WHEN Impact_Count >= 4 THEN cArea ELSE 0 END) AS Impact_4_Area,
                   SUM(cArea) AS Total_Area
            FROM ImpactCounts
            GROUP BY countryCode
        )
        SELECT countryCode,
               Impact_0_Area,
               ROUND(Impact_0_Area * 100.0 / NULLIF(Total_Area, 0), 0) AS Impact_0_Percent,
               Impact_1_Area,
               ROUND(Impact_1_Area * 100.0 / NULLIF(Total_Area, 0), 0) AS Impact_1_Percent,
               Impact_2_Area,
               ROUND(Impact_2_Area * 100.0 / NULLIF(Total_Area, 0), 0) AS Impact_2_Percent,
               Impact_3_Area,
               ROUND(Impact_3_Area * 100.0 / NULLIF(Total_Area, 0), 0) AS Impact_3_Percent,
               Impact_4_Area,
               ROUND(Impact_4_Area * 100.0 / NULLIF(Total_Area, 0), 0) AS Impact_4_Percent
        FROM AggregatedImpacts;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def swChemical_by_Country_2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts chemical status data by country.
    """

    output_file = os.path.join(working_directory, '14.swChemical_by_Country.csv')
    headers = ["Country", "Year", "Chemical Status Value", "Number", "Number (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   COUNT(swChemicalStatusValue) AS total_count
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND naturalAWBHMWB <> 'Unpopulated'
            GROUP BY countryCode
        )
        SELECT f.countryCode, 
               f.cYear,
               f.swChemicalStatusValue,
               COUNT(f.swChemicalStatusValue) AS Number,
               ROUND(
                   COUNT(f.swChemicalStatusValue) * 100.0 / NULLIF(t.total_count, 0), 0
               ) AS Percentage
        FROM SOW_SWB_SurfaceWaterBody f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.naturalAWBHMWB <> 'Unpopulated'
        GROUP BY f.countryCode, f.cYear, f.swChemicalStatusValue;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwSignificantImpactType2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts significant impact type data for groundwater bodies.
    """

    output_file = os.path.join(working_directory, '5.gwSignificantImpactType2016.csv')
    headers = ["Country", "Year", "Significant Impact Type", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve distinct Significant Impact Types**
    cur.execute("""
        SELECT DISTINCT gwSignificantImpactType
        FROM SOW_GWB_gwSignificantImpactType
        WHERE cYear = ? 
          AND gwSignificantImpactType <> 'Unpopulated'
    """, (cYear,))
    impact_types = [row[0] for row in cur.fetchall()]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCountryArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS country_total_area
            FROM SOW_GWB_gwSignificantImpactType
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
              AND gwQuantitativeStatusValue <> 'Unpopulated'
              AND gwChemicalStatusValue <> 'Unpopulated'
            GROUP BY countryCode
        ),
        TotalGlobalArea AS (
            SELECT SUM(country_total_area) AS global_total_area
            FROM TotalCountryArea
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwSignificantImpactType,
               ROUND(SUM(f.cArea), 0) AS Impact_Area,
               ROUND(
                   SUM(f.cArea) * 100.0 / NULLIF(g.global_total_area, 0), 0
               ) AS Impact_Percentage
        FROM SOW_GWB_gwSignificantImpactType f
        JOIN TotalCountryArea t 
          ON f.countryCode = t.countryCode
        JOIN TotalGlobalArea g 
          ON 1=1
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwSignificantImpactType IN ({','.join('?' * len(impact_types))})
          AND f.gwQuantitativeStatusValue <> 'Unpopulated'
          AND f.gwChemicalStatusValue <> 'Unpopulated'
        GROUP BY f.countryCode, f.cYear, f.gwSignificantImpactType;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + impact_types)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwSignificantImpactType_Other(db_file, countryCode, cYear, working_directory):
    """
    Extracts significant impact 'Other' types for groundwater bodies
    """

    output_file = os.path.join(working_directory, '5.gwSignificantImpactType_Other.csv')
    headers = ["Country", "Year", "Significant Impact Other", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve distinct Significant Impact "Other" Types**
    cur.execute("""
        SELECT DISTINCT gwSignificantImpactOther
        FROM SOW_GWB_gwSignificantImpactOther
        WHERE cYear = ? 
          AND gwSignificantImpactOther <> 'Unpopulated'
    """, (cYear,))
    impact_types = [row[0] for row in cur.fetchall()]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCountryArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS country_total_area
            FROM SOW_GWB_gwSignificantImpactOther
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
            GROUP BY countryCode
        ),
        TotalGlobalArea AS (
            SELECT SUM(country_total_area) AS global_total_area
            FROM TotalCountryArea
        )
        SELECT f.countryCode, 
               f.cYear,
               f.gwSignificantImpactOther,
               ROUND(SUM(f.cArea), 0) AS Impact_Area,
               ROUND(
                   SUM(f.cArea) * 100.0 / NULLIF(g.global_total_area, 0), 0
               ) AS Impact_Percentage
        FROM SOW_GWB_gwSignificantImpactOther f
        JOIN TotalCountryArea t 
          ON f.countryCode = t.countryCode
        JOIN TotalGlobalArea g 
          ON 1=1
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwSignificantImpactOther IN ({','.join('?' * len(impact_types))})
        GROUP BY f.countryCode, f.cYear, f.gwSignificantImpactOther;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + impact_types)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def SOW_GWB_gwSignificantPressureType_NumberOfImpact_by_country(db_file, countryCode, cYear, working_directory):
    """
    Extracts Significant Pressure Types
    """

    output_file = os.path.join(working_directory, '5.SOW_GWB_gwSignificantPressureType_NumberOfImpact_by_country.csv')
    headers = ['Country', 'Number of Impacts', 'Area (km^2)', 'Percent (%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve distinct Significant Pressure Types**
    cur.execute("""
        SELECT DISTINCT gwSignificantPressureOther
        FROM SOW_GWB_gwSignificantPressureOther
        WHERE cYear = ? 
          AND gwSignificantPressureOther <> 'Unpopulated'
    """, (cYear,))
    pressure_types = [row[0] for row in cur.fetchall()]

    # 🚀 **Optimized Query using CTE**
    query = f"""
        WITH TotalCountryArea AS (
            SELECT countryCode, 
                   SUM(cArea) AS country_total_area
            FROM SOW_GWB_gwSignificantPressureOther
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))}) 
            GROUP BY countryCode
        ),
        TotalGlobalArea AS (
            SELECT SUM(country_total_area) AS global_total_area
            FROM TotalCountryArea
        )
        SELECT f.countryCode, 
               f.gwSignificantPressureOther,
               ROUND(SUM(f.cArea), 0) AS Impact_Area,
               ROUND(
                   SUM(f.cArea) * 100.0 / NULLIF(g.global_total_area, 0), 0
               ) AS Impact_Percentage
        FROM SOW_GWB_gwSignificantPressureOther f
        JOIN TotalCountryArea t 
          ON f.countryCode = t.countryCode
        JOIN TotalGlobalArea g 
          ON 1=1
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwSignificantPressureOther IN ({','.join('?' * len(pressure_types))})
        GROUP BY f.countryCode, f.gwSignificantPressureOther;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + pressure_types)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def gwSignificantPressureType2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Significant Pressure Type
    """

    output_file = os.path.join(working_directory, '5.gwSignificantPressureType2016.csv')
    headers = ['Country', 'Significant Pressure Type Group', 'Significant Pressure Type', 'Area (km^2)', 'Area (%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total area per country (only for impacted groundwater bodies)**
    cur.execute("""
        SELECT countryCode, SUM(cArea) AS total_country_area
        FROM SOW_GWB_gwSignificantPressureType
        WHERE cYear = ? 
          AND countryCode IN ({})
          AND gwSignificantPressureType <> 'Unpopulated'
        GROUP BY countryCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)
    
    country_area_dict = {row[0]: row[1] for row in cur.fetchall()}  # Store total impacted area per country

    # 🚀 **Optimized Query using CTE**
    query = f"""
        SELECT f.countryCode, 
               f.gwSignificantPressureTypeGroup,
               f.gwSignificantPressureType,
               ROUND(SUM(f.cArea), 0) AS Impact_Area
        FROM SOW_GWB_gwSignificantPressureType f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwSignificantPressureType <> 'Unpopulated'
        GROUP BY f.countryCode, f.gwSignificantPressureTypeGroup, f.gwSignificantPressureType;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Area (%)` Per Country (Ensuring it sums to 100%)**
    formatted_data = []
    for country, pressure_group, pressure_type, impact_area in data:
        total_area = country_area_dict.get(country, 0)  # Avoid division by zero
        impact_percentage = round((impact_area * 100.0) / total_area, 0) if total_area else 0
        formatted_data.append([country, pressure_group, pressure_type, impact_area, impact_percentage])

    # **📌 Normalize Percentages to 100% per Country**
    country_sums = {}
    for row in formatted_data:
        country_sums[row[0]] = country_sums.get(row[0], 0) + row[4]

    for row in formatted_data:
        row[4] = round((row[4] * 100) / country_sums[row[0]], 0) if country_sums[row[0]] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def gwSignificantPressureType_OtherTable2016(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Significant Pressure Other Table
    """

    output_file = os.path.join(working_directory, f'5.gwSignificantPressureType_OtherTable{cYear}.csv')
    headers = ['Country', 'Year', 'Significant Pressure Other', 'Area (km^2)', 'Area(%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total impacted area per country**
    cur.execute("""
        SELECT countryCode, SUM(cArea) AS total_country_area
        FROM SOW_GWB_gwSignificantPressureOther
        WHERE cYear = ? 
          AND countryCode IN ({})
        GROUP BY countryCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)

    country_area_dict = {row[0]: row[1] for row in cur.fetchall()}  # Store total area per country

    # **🚀 Optimized Query using CTE**
    query = f"""
        SELECT f.countryCode, 
               f.cYear,
               f.gwSignificantPressureOther,
               ROUND(SUM(f.cArea), 0) AS Impact_Area
        FROM SOW_GWB_gwSignificantPressureOther f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
        GROUP BY f.countryCode, f.cYear, f.gwSignificantPressureOther;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Area (%)` Per Country**
    formatted_data = []
    for country, year, pressure_other, impact_area in data:
        total_area = country_area_dict.get(country, 0)  # Avoid division by zero
        impact_percentage = round((impact_area * 100.0) / total_area, 0) if total_area else 0
        formatted_data.append([country, year, pressure_other, impact_area, impact_percentage])

    # **📌 Normalize Percentages to 100% per Country**
    country_sums = {}
    for row in formatted_data:
        country_sums[row[0]] = country_sums.get(row[0], 0) + row[4]

    for row in formatted_data:
        row[4] = round((row[4] * 100) / country_sums[row[0]], 0) if country_sums[row[0]] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def SOW_GWB_gwPollutant_Table(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Pollutants, ensuring Area (%) is correctly calculated per country.
    """

    output_file = os.path.join(working_directory, '21.SOW_GWB_gwPollutant_Table2016.csv')
    headers = ["Country", "Pollutant", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total impacted area per country**
    cur.execute("""
        SELECT countryCode, SUM(cArea) AS total_impacted_area
        FROM SOW_GWB_gwPollutant
        WHERE cYear = ? 
          AND countryCode IN ({})
          AND gwPollutantCausingFailure = 'Yes'
        GROUP BY countryCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)

    country_area_dict = {row[0]: row[1] for row in cur.fetchall()}  # Store total impacted area per country

    # **🚀 Optimized Query using CTE**
    query = f"""
        SELECT f.countryCode, 
               f.gwPollutantCode,
               ROUND(SUM(f.cArea), 0) AS Pollutant_Area
        FROM SOW_GWB_gwPollutant f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwPollutantCausingFailure = 'Yes'
        GROUP BY f.countryCode, f.gwPollutantCode;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Area (%)` Per Country (Ensuring it sums to 100%)**
    formatted_data = []
    for country, pollutant, pollutant_area in data:
        total_area = country_area_dict.get(country, 0)  # Avoid division by zero
        impact_percentage = round((pollutant_area * 100.0) / total_area, 0) if total_area else 0
        formatted_data.append([country, pollutant, pollutant_area, impact_percentage])

    # **📌 Normalize Percentages to 100% per Country**
    country_sums = {}
    for row in formatted_data:
        country_sums[row[0]] = country_sums.get(row[0], 0) + row[3]

    for row in formatted_data:
        row[3] = round((row[3] * 100) / country_sums[row[0]], 0) if country_sums[row[0]] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()
            
def Surface_water_bodies_Failing_notUnknown_by_Country(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Surface Water Bodies Failing (excluding 'Unknown') by Country using CTEs.
    """

    if not countryCode:
        print("❌ No country codes provided.")
        return

    output_file = os.path.join(working_directory, f"16.Surface_water_bodies_Failing_notUnknown_by_Country{cYear}.csv")
    headers = ["Country", "Known Status", "Failing Status", "Failing (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # 🚀 **Optimized Query Using CTEs**
    query = f"""
        WITH TotalKnown AS (
            SELECT countryCode, 
                   SUM(cArea) AS known_status
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swChemicalStatusValue <> 'Unpopulated'
            GROUP BY countryCode
        ),
        FailingCounts AS (
            SELECT countryCode, 
                   SUM(cArea) AS failing_status
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ?
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND swChemicalStatusValue = '3'  -- Only failing status
            GROUP BY countryCode
        )
        SELECT tk.countryCode, 
               ROUND(tk.known_status, 0) AS Known_Status,
               ROUND(COALESCE(fc.failing_status, 0), 0) AS Failing_Status,
               ROUND(COALESCE(fc.failing_status, 0) * 100.0 / NULLIF(tk.known_status, 0), 0) AS Failing_Percentage
        FROM TotalKnown tk
        LEFT JOIN FailingCounts fc ON tk.countryCode = fc.countryCode
        ORDER BY tk.countryCode;
    """

    # Execute query
    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **Write to CSV**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()
    
def SOW_GWB_gwPollutant_Table_Other(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Pollutants reported as 'Other', ensuring Area (%) is correctly calculated per country.
    """

    output_file = os.path.join(working_directory, f'21.SOW_GWB_gwPollutant_Table{cYear}_Other.csv')
    headers = ["Country", "Pollutant reported as 'Other'", "Area (km^2)", "Area (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total impacted area per country**
    cur.execute("""
        SELECT countryCode, SUM(cArea) AS total_impacted_area
        FROM SOW_GWB_gwPollutantOther
        WHERE cYear = ? 
          AND countryCode IN ({})
          AND gwPollutantCausingFailure = 'Yes'
        GROUP BY countryCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)

    country_area_dict = {row[0]: row[1] for row in cur.fetchall()}  # Store total impacted area per country

    # **🚀 Optimized Query using CTE**
    query = f"""
        SELECT f.countryCode, 
               f.gwPollutantOther,
               ROUND(SUM(f.cArea), 0) AS Pollutant_Area
        FROM SOW_GWB_gwPollutantOther f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwPollutantCausingFailure = 'Yes'
        GROUP BY f.countryCode, f.gwPollutantOther;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Area (%)` Per Country (Ensuring it sums to 100%)**
    formatted_data = []
    for country, pollutant_other, pollutant_area in data:
        total_area = country_area_dict.get(country, 0)  # Avoid division by zero
        impact_percentage = round((pollutant_area * 100.0) / total_area, 0) if total_area else 0
        formatted_data.append([country, pollutant_other, pollutant_area, impact_percentage])

    # **📌 Normalize Percentages to 100% per Country**
    country_sums = {}
    for row in formatted_data:
        country_sums[row[0]] = country_sums.get(row[0], 0) + row[3]

    for row in formatted_data:
        row[3] = round((row[3] * 100) / country_sums[row[0]], 0) if country_sums[row[0]] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def swRiver_basin_specific_pollutants_reported_as_Other(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Surface Water Bodies Failing River Basin Specific Pollutants reported as 'Other'
    """

    output_file = os.path.join(working_directory, f'40.Surface_water_bodies_River_basin_specific_pollutants_reported_as_Other{cYear}.csv')
    headers = ["Country", "Year", "Failing RBSP Other", "Number", "Number (%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total number per country**
    cur.execute("""
        SELECT countryCode, COUNT(swFailingRBSP) AS total_count
        FROM SOW_SWB_FailingRBSPOther
        WHERE cYear = ? 
          AND countryCode IN ({})
        GROUP BY countryCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)

    country_total_dict = {row[0]: row[1] for row in cur.fetchall()}  # Store total count per country

    # **🚀 Optimized Query using CTE**
    query = f"""
        SELECT f.countryCode, 
               f.cYear,
               f.swFailingRBSPOther,
               COUNT(f.swFailingRBSP) AS Number
        FROM SOW_SWB_FailingRBSPOther f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
        GROUP BY f.countryCode, f.cYear, f.swFailingRBSPOther;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Number (%)` Per Country (Ensuring it sums to 100%)**
    formatted_data = []
    for country, year, failing_rbsp_other, number in data:
        total_count = country_total_dict.get(country, 0)  # Avoid division by zero
        percentage = round((number * 100.0) / total_count, 0) if total_count else 0
        formatted_data.append([country, year, failing_rbsp_other, number, percentage])

    # **📌 Normalize Percentages to 100% per Country**
    country_sums = {}
    for row in formatted_data:
        country_sums[row[0]] = country_sums.get(row[0], 0) + row[4]

    for row in formatted_data:
        row[4] = round((row[4] * 100) / country_sums[row[0]], 0) if country_sums[row[0]] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def Ground_water_bodies_Failing_notUnknown_by_Country(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Groundwater Bodies Failing (excluding unknown status)
    """

    output_file = os.path.join(working_directory, f'23.Ground_water_bodies_Failing_notUnknown_by_Country{cYear}.csv')
    headers = ["Country", "Known Status", "Failing status", "Failing(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total known area per country**
    cur.execute("""
        SELECT countryCode, SUM(cArea) AS total_known_area
        FROM SOW_GWB_GroundWaterBody
        WHERE cYear = ? 
          AND countryCode IN ({})
          AND gwChemicalStatusValue <> 'unknown'
        GROUP BY countryCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)

    country_total_dict = {row[0]: row[1] for row in cur.fetchall()}  # Store total known area per country

    # **🚀 Optimized Query using CTE**
    query = f"""
        SELECT f.countryCode, 
               ROUND(SUM(f.cArea), 0) AS Known_Status,
               ROUND(SUM(CASE WHEN f.gwChemicalStatusValue = '3' OR f.gwQuantitativeStatusValue = '3' THEN f.cArea ELSE 0 END), 0) AS Failing_Status
        FROM SOW_GWB_GroundWaterBody f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.gwChemicalStatusValue <> 'unknown'
        GROUP BY f.countryCode;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Failing (%)` Per Country**
    formatted_data = []
    for country, known_status, failing_status in data:
        total_area = country_total_dict.get(country, 0)  # Avoid division by zero
        failing_percentage = round((failing_status * 100.0) / total_area, 0) if total_area else 0
        formatted_data.append([country, known_status, failing_status, failing_percentage])

    # **📌 Normalize Percentages to 100% per Country**
    country_sums = {}
    for row in formatted_data:
        country_sums[row[0]] = country_sums.get(row[0], 0) + row[3]

    for row in formatted_data:
        row[3] = round((row[3] * 100) / country_sums[row[0]], 0) if country_sums[row[0]] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()
    
def Surface_water_bodies_QE1_Biological_quality_elements_assessment(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for QE1 Biological Quality Elements Monitoring Assessment.
    """

    output_file = os.path.join(working_directory, '42.Surface_water_bodies_QE1_Biological_quality_elements_assessment2016.csv')
    headers = ["Country", "Monitoring Results", "Code", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total number of surface water bodies per country and QE1 code**
    cur.execute("""
        SELECT countryCode, qeCode, COUNT(euSurfaceWaterBodyCode) AS total_count
        FROM SOW_SWB_QualityElement
        WHERE cYear = ? 
          AND countryCode IN ({})
          AND qeCode LIKE 'QE1%'
        GROUP BY countryCode, qeCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)

    country_qe_totals = {(row[0], row[1]): row[2] for row in cur.fetchall()}  # Store total counts per country and QE1 code

    # **🚀 Optimized Query for Data Extraction**
    query = f"""
        SELECT f.countryCode, 
               f.qeMonitoringResults, 
               f.qeCode, 
               COUNT(f.euSurfaceWaterBodyCode) AS Number
        FROM SOW_SWB_QualityElement f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.qeCode LIKE 'QE1%'
        GROUP BY f.countryCode, f.qeMonitoringResults, f.qeCode;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Number (%)` Per Country & QE1 Code**
    formatted_data = []
    for country, monitoring_result, qe_code, number in data:
        total_count = country_qe_totals.get((country, qe_code), 0)  # Avoid division by zero
        percentage = round((number * 100.0) / total_count, 0) if total_count else 0
        formatted_data.append([country, monitoring_result, qe_code, number, percentage])

    # **📌 Normalize Percentages to 100% per Country & QE1 Code**
    country_qe_sums = {}
    for row in formatted_data:
        key = (row[0], row[2])  # (Country, QE1 Code)
        country_qe_sums[key] = country_qe_sums.get(key, 0) + row[4]

    for row in formatted_data:
        key = (row[0], row[2])
        row[4] = round((row[4] * 100) / country_qe_sums[key], 0) if country_qe_sums[key] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def Surface_water_bodies_QE2_assessment(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for QE2 Biological Quality Elements Monitoring Assessment.
    """

    output_file = os.path.join(working_directory, '42.Surface_water_bodies_QE2_assessment2016.csv')
    headers = ["Country", "Monitoring Results", "Code", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve total number of surface water bodies per country and QE2 code**
    cur.execute("""
        SELECT countryCode, qeCode, COUNT(euSurfaceWaterBodyCode) AS total_count
        FROM SOW_SWB_QualityElement
        WHERE cYear = ? 
          AND countryCode IN ({})
          AND qeCode LIKE 'QE2%'
        GROUP BY countryCode, qeCode
    """.format(",".join(["?"] * len(countryCode))), [cYear] + countryCode)
    
    country_qe_totals = {(row[0], row[1]): row[2] for row in cur.fetchall()}  # Store total counts per country and QE2 code

    # **🚀 Optimized Query for Data Extraction**
    query = f"""
        SELECT f.countryCode, 
               f.qeMonitoringResults, 
               f.qeCode, 
               COUNT(f.euSurfaceWaterBodyCode) AS Number
        FROM SOW_SWB_QualityElement f
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.qeCode LIKE 'QE2%'
        GROUP BY f.countryCode, f.qeMonitoringResults, f.qeCode;
    """

    cur.execute(query, [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Compute the Correct `Number (%)` Per Country & QE2 Code**
    formatted_data = []
    for country, monitoring_result, qe_code, number in data:
        total_count = country_qe_totals.get((country, qe_code), 0)  # Avoid division by zero
        percentage = round((number * 100.0) / total_count, 0) if total_count else 0
        formatted_data.append([country, monitoring_result, qe_code, number, percentage])

    # **📌 Normalize Percentages to 100% per Country & QE2 Code**
    country_qe_sums = {}
    for row in formatted_data:
        key = (row[0], row[2])  # (Country, QE2 Code)
        country_qe_sums[key] = country_qe_sums.get(key, 0) + row[4]

    for row in formatted_data:
        key = (row[0], row[2])
        row[4] = round((row[4] * 100) / country_qe_sums[key], 0) if country_qe_sums[key] else 0

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(formatted_data)

    conn.close()

def Surface_water_bodies_QE3_assessment(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Surface Water Bodies QE3 Monitoring Assessment.
    """

    output_file = os.path.join(working_directory, f'42.Surface_water_bodies_QE3_assessment{cYear}.csv')
    headers = ["Country", "Monitoring Results", "Code", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve distinct QE3-1 codes**
    query_qe_codes = """
        SELECT DISTINCT qeCode 
        FROM SOW_SWB_QualityElement
        WHERE cYear = ? 
          AND swEcologicalStatusOrPotentialValue <> 'unpopulated' 
          AND naturalAWBHMWB <> 'unpopulated' 
          AND qeCode LIKE 'QE3-1%'
    """
    cur.execute(query_qe_codes, (cYear,))
    qe_codes = [row[0] for row in cur.fetchall()]

    if not qe_codes:
        print(f"⚠️ No QE3-1 quality elements found for {cYear}.")
        conn.close()
        return

    # Possible monitoring results
    monitoring_results = ["Monitoring", "Grouping", "Expert judgement", "Unpopulated"]

    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, qeCode, COUNT(euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_QualityElement
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND qeCode IN ({','.join('?' * len(qe_codes))})
            GROUP BY countryCode, qeCode
        )
        SELECT f.countryCode, 
               f.qeMonitoringResults, 
               f.qeCode, 
               COUNT(f.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(f.euSurfaceWaterBodyCode) * 100.0 / NULLIF(t.total_count, 0), 1
               ) AS Percentage
        FROM SOW_SWB_QualityElement f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode AND f.qeCode = t.qeCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.qeCode IN ({','.join('?' * len(qe_codes))})
          AND f.qeMonitoringResults IN ({','.join('?' * len(monitoring_results))})
        GROUP BY f.countryCode, f.qeMonitoringResults, f.qeCode;
    """

    cur.execute(query, [cYear] + countryCode + qe_codes + [cYear] + countryCode + qe_codes + monitoring_results)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()
    
def Surface_water_bodies_QE3_3_assessment(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for Surface Water Bodies QE3-3 Monitoring Assessment.
    """

    output_file = os.path.join(working_directory, f'42.Surface_water_bodies_QE3_3_assessment{cYear}.csv')
    headers = ["Country", "Monitoring Results", "Code", "Number", "Number(%)"]

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **Retrieve distinct QE3-3 codes**
    query_qe_codes = """
        SELECT DISTINCT qeCode 
        FROM SOW_SWB_QualityElement
        WHERE cYear = ? 
          AND swEcologicalStatusOrPotentialValue <> 'unpopulated' 
          AND naturalAWBHMWB <> 'unpopulated' 
          AND qeCode LIKE 'QE3-3%'
    """
    cur.execute(query_qe_codes, (cYear,))
    qe_codes = [row[0] for row in cur.fetchall()]

    if not qe_codes:
        print(f"⚠️ No QE3-3 quality elements found for {cYear}.")
        conn.close()
        return

    # Possible monitoring results
    monitoring_results = ["Monitoring", "Grouping", "Expert judgement", "Unpopulated"]

    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, qeCode, COUNT(euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_QualityElement
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND qeCode IN ({','.join('?' * len(qe_codes))})
            GROUP BY countryCode, qeCode
        )
        SELECT f.countryCode, 
               f.qeMonitoringResults, 
               f.qeCode, 
               COUNT(f.euSurfaceWaterBodyCode) AS Number,
               ROUND(
                   COUNT(f.euSurfaceWaterBodyCode) * 100.0 / NULLIF(t.total_count, 0), 0
               ) AS Percentage
        FROM SOW_SWB_QualityElement f
        JOIN TotalCounts t 
          ON f.countryCode = t.countryCode AND f.qeCode = t.qeCode
        WHERE f.cYear = ? 
          AND f.countryCode IN ({','.join('?' * len(countryCode))}) 
          AND f.qeCode IN ({','.join('?' * len(qe_codes))})
          AND f.qeMonitoringResults IN ({','.join('?' * len(monitoring_results))})
        GROUP BY f.countryCode, f.qeMonitoringResults, f.qeCode;
    """

    cur.execute(query, [cYear] + countryCode + qe_codes + [cYear] + countryCode + qe_codes + monitoring_results)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()

def sw_delineation_of_the_management_units_in_the_1st_and_2nd_RBMP(db_file, countryCode, cYear, working_directory):
    """
    Extracts data for delineation of management units in the 1st and 2nd RBMP.
    Ensures percentage calculations are done per country.
    """

    output_file = os.path.join(working_directory, f'9.1.sw_delineation_of_the_management_units_in_the_1st_and_2nd_RBMP_Unchanged_{cYear}.csv')
    headers = ['Country', 'Year', 'Unchanged', 'Unchanged (%)', 'Other', 'Other (%)']

    conn = create_connection(db_file)
    if conn is None:
        print("❌ Database connection failed.")
        return

    cur = conn.cursor()

    # **🚀 Optimized SQL Query using CTE**
    query = f"""
        WITH TotalCounts AS (
            SELECT countryCode, 
                   cYear, 
                   COUNT(euSurfaceWaterBodyCode) AS total_count
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
            GROUP BY countryCode, cYear
        ),
        Unchange AS (
            SELECT countryCode, 
                   cYear, 
                   COUNT(euSurfaceWaterBodyCode) AS UnchangeCount
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND wiseEvolutionType IN ('noChange', 'changeCode', 'change')
            GROUP BY countryCode, cYear
        ),
        Other AS (
            SELECT countryCode, 
                   cYear, 
                   COUNT(euSurfaceWaterBodyCode) AS OtherCount
            FROM SOW_SWB_SurfaceWaterBody
            WHERE cYear = ? 
              AND countryCode IN ({','.join('?' * len(countryCode))})
              AND wiseEvolutionType NOT IN ('noChange', 'changeCode', 'change')
            GROUP BY countryCode, cYear
        )
        SELECT t.countryCode, 
               t.cYear, 
               COALESCE(u.UnchangeCount, 0) AS Unchanged, 
               ROUND(COALESCE(u.UnchangeCount, 0) * 100.0 / NULLIF(t.total_count, 0), 0) AS Unchanged_Percent,
               COALESCE(o.OtherCount, 0) AS Other, 
               ROUND(COALESCE(o.OtherCount, 0) * 100.0 / NULLIF(t.total_count, 0), 0) AS Other_Percent
        FROM TotalCounts t
        LEFT JOIN Unchange u ON t.countryCode = u.countryCode AND t.cYear = u.cYear
        LEFT JOIN Other o ON t.countryCode = o.countryCode AND t.cYear = o.cYear;
    """

    cur.execute(query, [cYear] + countryCode + [cYear] + countryCode + [cYear] + countryCode)
    data = cur.fetchall()

    # **📌 Write Data to CSV**
    with open(output_file, 'w+', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    conn.close()
    
