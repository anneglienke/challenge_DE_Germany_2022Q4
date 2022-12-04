import hashlib

# Generate an MD5 hash which is a concatenation of all arguments passed to the function
def generateSongId(*args):
  arglist = [item for item in args]
  song_id = ''.join(arglist)

  return hashlib.md5(song_id.encode()).hexdigest()
    