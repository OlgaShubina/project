import simplejson as json
from collections import defaultdict
import http.client
import logging 

class Req(object):
	"""docstring for """
	def __init__(self, url, host, localhost):
		self.url = url
		self.host = host
		self.localhost = localhost

	def post_freq(self, data, uuid):
		data_js = json.dumps(data).encode('utf-8')
		headers = {'Content-type': 'application/json'}
		conn = http.client.HTTPConnection(self.host, self.localhost)
		conn.request("POST", "/chunk/"+str(uuid), data_js, headers)
		response = conn.getresponse()
		return response

	def get_freq(self, response):
			
		js = json.load(response)
		
		next = js.get("next")
		

		return js

	def get_chunk(self, uuid):

		headers = {'Content-type': 'application/json'}
		conn = http.client.HTTPConnection(self.host, self.localhost)
		conn.request("GET", "/chunk/"+str(uuid), {}, headers)		
		response = conn.getresponse()
		return response	

	def Cursor(self, t1, t2, channels):
		try:
			data = {"t1" : t1, "t2" : t2, "channels" :  channels}

			data_js = json.dumps(data).encode('utf-8')
			headers = {'Content-type': 'application/json'}
			conn = http.client.HTTPConnection(self.host, self.localhost)
			conn.request("POST", "/cursor", data_js, headers)
			response = conn.getresponse()
			js = json.loads(response.read())
			
			count = 0
			current_uuid = js["first"]["uuid"]
			length = js["len_dict"]
			print(js)
			while(count<length):
				r =self.get_chunk(current_uuid)
				current_chunk = self.get_freq(r)
				
				yield current_chunk
				current_uuid = current_chunk.get("next", None)
				
				if current_uuid is None:
					break
				
				count+=1
			return js
		except ValueError or KeyError:
			logging.error("time is not correct")


if __name__ =='__main__':
	req = Req("http://127.0.0.1:5000/", "127.0.0.1", 5000)
	#req.Cursor("2013-04-0", "2013-04-27 00:01:20.000000", [3076, 125])
	for chunk in req.Cursor("2013-04-27 00:00:00.00000", "2013-04-27 00:01:20.000000", [48, 231]):
		print(chunk)
