import yaml

class Config(object):
	"""docstring for Congig"""
	def __init__(self, file_name):
		
		self.config = yaml.load(open(file_name))
		

