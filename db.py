import sqlite3

# Path and file name to store the SQLite database
DATABASE_FILE = './data/song_plays.db'

# Table creation script
EVENTS_TABLE_CREATE = '''
  CREATE TABLE IF NOT EXISTS song_plays(
    song_play_id TEXT NOT NULL PRIMARY KEY,
    year_month INTEGER NOT NULL,
    listened_at INTEGER NOT NULL,
    artist_id TEXT NOT NULL,
    track_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    user_id TEXT NOT NULL
    artist_name TEXT NOT NULL,
    track_name TEXT NOT NULL,
    release_name TEXT NOT NULL,
    user_name TEXT NOT NULL
  );
'''

YEAR_MONTH_INDEX = 'CREATE INDEX IF NOT EXISTS year_month_index ON song_plays (year_month);'

conn = sqlite3.connect(DATABASE_FILE)

# Execute a query
def executeQuery(sql):
  with conn:
    conn.execute(sql)

# Execute a query and print each resulting row
def executeAndPrint(sql):
  cursor = conn.execute(sql)
  for row in cursor:
    print(row)

# Insert rows in bulk
def insertMany(sql, data):
  with conn:
    conn.executemany(sql, data)

# Execute the table creation script
def createTable():
  executeQuery(EVENTS_TABLE_CREATE)
  executeQuery(YEAR_MONTH_INDEX)