# coding=utf8

def sstr(obj):
	""" converts any object to str, if necessary encodes unicode chars """
	try:
		return str(obj)
	except UnicodeEncodeError:
		return unicode(obj).encode('utf-8')

class Event(object):
	
	def __init__(self):
		self.handlers = []
	
	def add(self, handler):
		self.handlers.append(handler)
		return self
	
	def remove(self, handler):
		self.handlers.remove(handler)
		return self
	
	def fire(self, sender, earg=None):
		for handler in self.handlers:
			handler(sender, earg)
	
	__iadd__ = add
	__isub__ = remove
	__call__ = fire