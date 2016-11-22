import psycopg2 
from collections import namedtuple, defaultdict

LogDouble = namedtuple('LogDouble','time, ch_id, value')

class Table(object):


    def __init__(self, name, conn):
        self._name = name
        self._quwery = "SELECT * FROM {} WHERE ch_id IN {} AND time BETWEEN CAST('{}' AS TIMESTAMP) AND CAST('{}' AS TIMESTAMP)"
        self._conn = conn

    """docstring fos Table"""
    def SelectTable(self, t1, t2, channels):
        
        with self._conn.cursor() as cur:
            if isinstance(channels, list):
                cur.execute(self._quwery.format(self._name, tuple(channels), t1, t2))
                data = [LogDouble(*row) for row in cur]
            else:
                cur.execute("SELECT * FROM {} WHERE ch_id={} AND time BETWEEN CAST('{}' AS TIMESTAMP) AND CAST('{}' AS TIMESTAMP)".format(self._name, channels, t1, t2))
                data = [LogDouble(*row) for row in cur]
        return data

class Channels(object):
    """docstring for Channels"""
    def __init__(self, Id, name, types):
        self.Id = Id
        self.name = name
        self.types = types

class Channels_(object):
    """docstring for Channels_"""
    def GetChannels(self, table):
        ch = Channels_()
        with table.cursor() as cur:
            cur.execute("SELECT id, name, log_type FROM channels")
            table_channels = [Channels(*row) for row in cur]  
            if table_channels is None:
                logging.error("table not created")
        return table_channels      

    def foundType(self, table_channels, channels):
        d = defaultdict(list)
        array = []
        for row in table_channels:
            if isinstance(channels, list):
                for i in channels:
                    if (row.Id == i) or (row.name == i):
                       array.append(Channels(row.Id, row.name, row.types))
                for ch in array:
                    d[ch.types].append(ch.Id)
            else:
                if (row.Id == channels) or (row.name == channels):
                    array.append(Channels(row.Id, row.name, row.types))
                    d[channels.types].append(channels.Id)
        return d
        

class Archive(object):
    """docstring for Archive"""

    def __init__(self, config):
        self.conn = psycopg2.connect("dbname={} user={} password={} host={}". format(config["database"]["dbname"], config["database"]["user"], config["database"]["password"], config["database"]["host"]))
        self._tables = {}
        self.table = config["table"]
        
        for dtype, table_name in config["table"].items():
          self._tables[dtype] = Table(table_name, self.conn)

    
    def GetData(self, channels, t1, t2): 
        data = []        
        ch = Channels_()
        table_channels = ch.GetChannels(self.conn)
        d = ch.foundType(table_channels, channels)        
        for t, chans in d.items():
              data += self._tables[t].SelectTable(t1, t2, chans)         
        return data
