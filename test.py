from flask import Flask, request, abort
import simplejson as json
import flask
from collections import defaultdict
import datetime
import Archive
import Query
import urllib
import req
import redis_storage
import parsconfig

#config = {"size_stream" : 8000, "density" : 100}
config = parsconfig.Config('/home/olga/projects/config.yaml')
data_base = Archive.Archive(config.config)

app = Flask(__name__)
app.debug = True
app.logger.debug('Значение для отладки')


@app.route("/")       
def main():   
	return flask.redirect("/chunk")
	
		

@app.route("/channels")
def channels():
	conn = data_base.conn
	channels = Archive.Channels_()
	table_channels = channels.GetChannels(data_base.conn)
	d = defaultdict(list)
	for i in table_channels:
		d[i.Id].append({"name" : str(i.name), "value" : str(i.types)})
	js = json.dumps(d)
	return js

@app.route("/cursor", methods = ["GET", "POST"])
def create_chunk():
	if request.method == 'POST':
		try:
			request_data = request.get_json()
			app.logger.debug(request_data)
			if request_data["t1"] > request_data["t2"]:
				app.logger.error("data")
				abort(400) 

			query = Query.Query(config.config["query"], request_data["t1"], request_data["t2"])
			chunk_list = query.Separate(request_data["channels"])
			chunk_dict = {"len_dict" : len(chunk_list), 
							"first" : {"t1" : chunk_list[0].t1,
										"t2" : chunk_list[0].t2, 
										"channels" : chunk_list[0].ch ,
							 			"uuid" : chunk_list[0].uuid}}

			js = json.dumps(chunk_dict)
			return js 
		except (TypeError, ValueError, KeyError) as ex:
			#abort(400)
			app.logger.error(str(ex))
			abort(400)

	else:
		abort(400)



@app.route("/chunk/<chunk_id>", methods = ["GET", "POST"])
def chunk(chunk_id):
	if request.method == 'GET':
		try:
			storage = redis_storage.RedisStorage()
			current_chunk = storage.get_chunk(chunk_id)
			if current_chunk is None:
				abort(404)
			data = data_base.GetData(current_chunk["channels"], 
										datetime.datetime.fromtimestamp(current_chunk["t1"]), 
										datetime.datetime.fromtimestamp(current_chunk["t2"]))
			if data is None:
				abort(404)
			d = defaultdict(list)
			for i in data:
				d[i[1]].append({"time" : i[0].timestamp(), "value" : i[2]})	
			if current_chunk["next"]:
				d.update({"next" : current_chunk["next"]})	
			js = json.dumps(d)
			app.logger.debug(current_chunk)
			return js
		except (TypeError, ValueError) as ex:
			
			app.logger.error(str(ex))
			abort(400)
	else:
		abort(400)

if __name__ =='__main__':
    app.run()     

  