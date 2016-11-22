import Archive
import parsconfig
from collections import namedtuple, defaultdict
import math
import psycopg2
import datetime
from collections import defaultdict 

LogDouble = namedtuple('LogDouble','time, ch_id, value')

class Average(object):
	"""docstring for Average"""
	def __init__(self, config):
		self.conn = psycopg2.connect("dbname={} user={} password={} host={}". format(config["database"]["dbname"], "logger", "logger", config["database"]["host"]))
		self.query_select = "SELECT * FROM {} WHERE time BETWEEN CAST('{}' AS TIMESTAMP) AND CAST('{}' AS TIMESTAMP)"
		self.query_insert = "INSERT INTO {} (time , ch_id, avg, mediana, max, min, sygma) VALUES ( CAST('{}' AS TIMESTAMP), {}, {}, {}, {}, {}, {})"
	def sort_value(self, value):
		return value

	def get_data(self, t1, t2):
		with self.conn.cursor() as cur:
			cur.execute(self.query_select.format("log_double", t1, t2))
			data = [LogDouble(*row) for row in cur]
		return data

	
	def sum_data(self, data):
		sum  = 0
		for i in data:
			sum+=i.value
		return sum

	def median_data(self, data):
		value = []
		for i in data:
			value.append(i.value)
		value.sort(key=self.sort_value)
		lenght = len(value)
		#print(lenght)
		if lenght%2==0 and lenght!=2:
			mediana = (value[lenght//2]+value[lenght//2+1])/2
		elif lenght ==1:
			mediana = value[0]
		elif lenght == 2:
			mediana = (value[0]+value[1])/2
		else:
			mediana = value[lenght//2+1]
		return mediana

	def min_data(self, data):
		value = []
		for i in data:
			value.append(i.value)
		value.sort(key=self.sort_value)
		return value[0]

	def max_data(self, data):
		value = []
		for i in data:
			value.append(i.value)
		value.sort(key=self.sort_value)
		value.reverse()
		return value[0]

	def sygma(self, data, lenght, average):
		summa = 0
		for i in data:
			summa += (i.value + average)**2
		sygma = math.sqrt(summa/lenght)
		return sygma

	def insert_data(self, time, ch_id, avg, mediana, min, max, syg):
		cur = self.conn.cursor()
		cur.execute(self.query_insert.format("average", time, ch_id, avg, mediana, min, max, syg))

	def compress_data(self, t1, t2, delta):
		t1_ = datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S.%f').timestamp()
		t2_ = datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S.%f').timestamp()
		ch_id = 0
		d = defaultdict(list)
		while t1_ < t2_:
			data = self.get_data(datetime.datetime.fromtimestamp(t1_), datetime.datetime.fromtimestamp(t1_+delta))
			for i in data:
				d[i.ch_id].append(i)
			for ch, i in d.items():
				lenght = len(data)
				avg = obj.sum_data(i)/lenght
				mediana = obj.median_data(i)
				minimum = obj.min_data(i)
				maximum = obj.max_data(i)
				syg = obj.sygma(i, lenght, avg)
				self.insert_data(datetime.datetime.fromtimestamp(t1_), ch, avg, mediana, minimum, maximum, syg)
			t1_+=delta
		self.conn.commit()

if __name__ =='__main__':
	config = parsconfig.Config('/home/olga/projects/config.yaml')
	#print(config)
	obj = Average(config.config["pg_archive"])
	data = obj.get_data("2013-04-27 00:00:00.00000","2013-04-27 00:01:00.00000")
	lenght = len(data)
	avg = obj.sum_data(data)/lenght
	mediana = obj.median_data(data)
	minimum = obj.min_data(data)
	maximum = obj.max_data(data)
	syg = obj.sygma(data, lenght, avg)	
	obj.compress_data("2013-04-27 00:00:00.00000", "2013-04-27 01:00:00.00000", 120.0)