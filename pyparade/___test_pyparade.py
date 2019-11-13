# coding=utf-8
from __future__ import print_function
import unittest, re, operator

import pyparade

class TestPyParade(unittest.TestCase):
	"""Uses a comination of map and reduceByKey to calculate occurencies of each word in a text.
	"""
	def test_wordcount(self):
		text = pyparade.Dataset(["abc test abc test test xyz", "abc test2 abc test cde xyz"])
		words = text.flat_map(lambda line: [(word, 1) for word in re.split(" ", line)])
		wordcounts = words.reduce_by_key(operator.add)

		result = wordcounts.collect(name="Counting words")
		print(result)
