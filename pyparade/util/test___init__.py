import random
import unittest

from pyparade.util import Event, ParMap, Timer

class TestEvent(unittest.TestCase):
	def test_fire_event(self):
		e = Event()
		self.fired = False
		
		def f(self, sender):
			global fired
			self.fired = True

		e += f #add listener
		e(self) #fire Event

		self.assertEqual(True, self.fired)

	def test_remove_listener(self):
		e = Event()
		self.fired = False
		
		def f(self, sender):
			global fired
			self.fired = True

		e += f #add listener
		e -= f #remove listener
		e(self) #fire Event

		self.assertEqual(False, self.fired)

class TestParMap(unittest.TestCase):
	"""Tests parallelized map"""

	def test_plus_one(self):
		def f(a):
			for i in range(0,1000):
				random.random()

			return ((a + 1) % 100000, a+1)

		p = ParMap(f)
		t_par = Timer("parmap")
		calculated_values = [v for v in p.map(range(1000000))]
		t_par.stop()
		t_map = Timer("map")
		correct_values = map(f,range(1000000))
		t_map.stop()
		self.assertLessEqual(t_par.seconds, 0.8*t_map.seconds)
		for calculated_value, correct_value in zip(calculated_values, correct_values):
			self.assertEqual(correct_value, calculated_value)