import json
import pandas as pd
import logging
from db import executeAndPrint, insertMany, createTable
from utils import generateSongId

logging.basicConfig(format='[%(asctime)s] %(message)s', level=logging.INFO)

# 50000 is an arbitrary number 
CHUNK_SIZE = 50000

INPUT_PATH = './dataset.txt'

def loadEvents():
  # Load the dataset in chunks of CHUNK_SIZE for better performance
  total = 0
  with pd.read_json(path_or_buf=INPUT_PATH, lines=True, chunksize=CHUNK_SIZE) as reader:
    for chunk in reader:
      total += loadChunk(chunk)
  logging.info(f'Processed {total} rows!')

def loadChunk(chunk: pd.DataFrame):
  logging.info('Started processing a new chunk of data')

  # Flatten track_metadata object
  dict_chunk = json.loads(chunk.to_json(orient="records"))
  df = pd.json_normalize(dict_chunk)

  # Rename columns to enable parameter binding on sqlite executemany
  df = df.rename(columns={
    "track_metadata.artist_id": "artist_id",
    "track_metadata.track_id": "track_id",
    "track_metadata.release_id": "release_id",
    "track_metadata.artist_name": "artist_name",
    "track_metadata.track_name": "track_name",
    "track_metadata.release_name": "release_name",
    "track_metadata.additional_info.dedup_tag": "dedup_tag"
  })

  # Fill empty values for dedup_tag with 0, meaning those will be the unique rows
  df.dedup_tag = df.dedup_tag.fillna(0)

  # Filter out duplicates
  df = df[df.dedup_tag < 1]

  # Drop all unused columns from the dataframe
  df = df[df.columns.drop("recording_msid")]
  df = df[df.columns.drop(list(df.filter(regex='track_metadata')))]

  # Replace None values with an empty string
  df = df.replace({
    None: '',
  })

  # Convert listened_at to an integer that can be used in queries on SQLite
  df.listened_at = df.listened_at / 1000
  df.listened_at = df.listened_at.astype(int)

  # Create year_month column with the year and month of that row (this column will be used as index)
  df['year_month'] = pd.to_datetime(df['listened_at'], unit='s').dt.strftime('%Y%m')
  df['year_month'] = pd.to_numeric(df['year_month'])

  recordings = df.to_dict('records')

  # Generate song_play_id by hashing track name, artist name, release name, listened at and user name
  valid_recordings = []
  invalid_recordings = []
  for rec in recordings:
    # Validity is defined by the presence of all data required to create a song_play_id
    is_valid = rec['track_id'] and rec['artist_id'] and rec['release_id'] and rec['listened_at'] and rec['user_id']

    if is_valid:
      rec['song_play_id'] = generateSongId(rec['track_id'], rec['artist_id'], rec['release_id'], str(int(rec["listened_at"])), rec['user_id'])
      valid_recordings.append(rec)
    else:
      invalid_recordings.append(rec)

  logging.info(f'There are {len(valid_recordings)} valid recordings and {len(invalid_recordings)} corrupt recordings (which will be discarded)')
  logging.info('Example valid row for the chunk that is now processed:')
  logging.info(valid_recordings[0])

  # Insert recordings chunk into the table
  insertMany(
    '''
      INSERT OR IGNORE INTO song_plays(song_play_id, listened_at, year_month, artist_id, track_id, release_id, user_id, artist_name, track_name, release_name, user_name)
      VALUES (:song_play_id, :listened_at, :year_month, :artist_name, :track_name, :release_name, :user_name)
    ''',
    valid_recordings
  )

  logging.info('Finished processing of the current chunk')

  # Return the amount of rows to keep count
  return len(valid_recordings)

if __name__ == "__main__":
  createTable()
  loadEvents()
  executeAndPrint('SELECT COUNT(DISTINCT(song_play_id)) FROM song_plays')
