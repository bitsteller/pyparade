# coding=utf-8
import unittest, re

class TestPyParade(unittest.TestCase):
	"""Uses a comination of map and reduceByKey to calculate occurencies of each word in a text.
	"""
	def test_wordcount(self):
		text = Dataset("abc test abc test test xyz")
		words = text.map([(word, 1) for word in re.split(str, " ")])
		counts = text.reduceByKey(add)

		p = ParallelProcess(counts, name="Counting words")
		p.run()
		print(p.collect())
