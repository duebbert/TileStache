""" Caches tiles to Redis

Requires redis-py and redis-server
  https://pypi.python.org/pypi/redis/
  http://redis.io/

  sudo apt-get install redis-server
  pip install redis


Example configuration:

  "cache": {
    "name": "Redis",
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "key prefix": "unique-id",
    "master host": "",
    "master port": 6379
  }

Redis cache parameters:

  host
    Defaults to "localhost" if omitted.

  port
    Integer; Defaults to 6379 if omitted.

  db
    Integer; Redis database number, defaults to 0 if omitted.

  key prefix
    Optional string to prepend to generated key.
    Useful when running multiple instances of TileStache
    that share the same Redis database to avoid key
    collisions (though the prefered solution is to use a different
    db number). The key prefix will be prepended to the
    key name. Defaults to "".
    
  master host
    Optional string for a redis master host. If this is set, then any writes
    will be done to this host while the reads will still use the normal
    settings. Otherwise writes and reads will be done to the same host.

  master port
    Integer; Defaults to 6379

"""
from time import time as _time, sleep as _sleep


try:
    import redis
except ImportError:
    # at least we can build the documentation
    pass


def tile_key(layer, coord, format, key_prefix):
    """ Return a tile key string.
    """
    name = layer.name()
    tile = '%(zoom)d/%(column)d/%(row)d' % coord.__dict__
    key = str('%(key_prefix)s/%(name)s/%(tile)s.%(format)s' % locals())
    return key


class Cache:
    """
    """
    def __init__(self, host="localhost", port=6379, db=0, key_prefix='',
                 master_host=None, master_port=6379):
        self.host = host
        self.port = port
        self.db = db
        self.conn_read = redis.Redis(host=self.host, port=self.port, db=self.db)
        self.key_prefix = key_prefix

        self.master_host = master_host
        self.master_port = master_port
        if self.master_host:
            self.conn_write = redis.Redis(host=self.master_host,
                                          port=self.master_port,
                                          db=self.db)
        else:
            self.conn_write = self.conn_read


    def lock(self, layer, coord, format):
        """ Acquire a cache lock for this tile.
            Returns nothing, but blocks until the lock has been acquired.
        """
        key = tile_key(layer, coord, format, self.key_prefix) + "-lock" 
        due = _time() + layer.stale_lock_timeout

        while _time() < due:
            if self.conn_write.setnx(key, 'locked.'):
                return

            _sleep(.2)

        self.conn_write.set(key, 'locked.')
        return
        
    def unlock(self, layer, coord, format):
        """ Release a cache lock for this tile.
        """
        key = tile_key(layer, coord, format, self.key_prefix)
        self.conn_write.delete(key+'-lock')
        
    def remove(self, layer, coord, format):
        """ Remove a cached tile.
        """
        key = tile_key(layer, coord, format, self.key_prefix)
        self.conn_write.delete(key)
        
    def read(self, layer, coord, format):
        """ Read a cached tile.
        """
        key = tile_key(layer, coord, format, self.key_prefix)
        value = self.conn_read.get(key)
        return value
        
    def save(self, body, layer, coord, format):
        """ Save a cached tile.
        """
        key = tile_key(layer, coord, format, self.key_prefix)
        self.conn_write.set(key, body)
