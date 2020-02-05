# coding=utf-8
from __future__ import print_function
import unittest, re, operator
import time, random


import pyparade

class TestPyParade(unittest.TestCase):
	"""Uses a comination of map and reduceByKey to calculate occurencies of each word in a text.
	"""
	def test_wordcount(self):
		text = pyparade.Dataset(["abc test abc test test xyz", "abc test2 abc test cde xyz"])
		words = text.flat_map(lambda line: [(word, 1) for word in re.split(" ", line)])
		wordcounts = words.reduce_by_key(operator.add)

		result = wordcounts.collect(name="Counting words")

		correctResult = [("abc", 4), ("test", 4), ("xyz", 2), ("test2", 1), ("cde", 1)]
		self.assertEqual(len(result), len(correctResult))

		for r in result:
			self.assertIn(r, correctResult)

	def test_map(self):
		d = pyparade.Dataset(list(range(0,100000)), name="Numbers with a really extremly unnecessarly long dataset name for no reason")
		
		def f(a):
			#print(str(a) + "->" + str(a+1))
			time.sleep(0.0001)
			return a + 1

		def g(a):
			#print(str(a) + "->" + str(a+1))
			time.sleep(0.001)
			return a + 1

		inc = d.map(f, name="add 1", output_name="Numbers+1").map(g, name="add 1", output_name="Numbers+2").collect()
		equal = [(1 if a == b else 0) for a, b in zip(inc, list(range(2,100002)))]
		self.assertEqual(sum(equal), 100000)

	def test_group(self):
		d = pyparade.Dataset(list(range(0,1000000)))

		def f(a):
			for i in range(0,500):
				random.random()

			return ((a + 1) % 10, a+1)

		def g(a):
			k,values = a
			return (k, sum(values)/len(values))

		result = d.map(f).group_by_key().map(g).collect()

		for i in range(0,10):
			self.assertEqual(result[i][0], i)
			self.assertTrue(abs(result[i][1]-500000)<=10)

	def test_fold(self):
		def f(a):
			#print(str(a) + "->" + str(a+1))
			#time.sleep(0.001)
			for i in range(0,1000):
				random.random()

			return ((a + 1) % 100000, a+1)

		def g(kv):
			k,v = kv
			return v

		result = pyparade.Dataset(list(range(0,1000000)), name="Numbers") \
					.map(f, name="calculate", output_name="Key/Value pairs") \
				 	.map(g, name="take value", output_name="Values") \
				 	.fold(0,operator.add,name="sum", output_name="Sum").collect()
		self.assertEqual(result[0], sum(range(1,1000001)))

	def test_map(self):
		def slow_generator():
			for i in xrange(1,100):
				time.sleep(0.1)
				yield i

		d = pyparade.Dataset(slow_generator, length=100, name="Slowly generated dataset")
		
		def f(a):
			#print(str(a) + "->" + str(a+1))
			time.sleep(0.0001)
			return a + 1

		inc = d.map(f, name="add 1", output_name="Numbers+1").collect()
		equal = [i in xrange(1,100)]
		self.assertEqual(sum(equal), 100)

	def test_batch(self):
		d = pyparade.Dataset(list(range(0,1000)), name="Numbers")

		batches = d.batch(10).collect()
		self.assertEqual(len(batches), 100)
		self.assertEqual(batches[0][0], 0)
		self.assertEqual(batches[99][9], 999)

