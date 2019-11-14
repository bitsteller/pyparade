from past.builtins import cmp
from builtins import str
from builtins import range
from builtins import object
from functools import total_ordering

import os, random, tempfile, shutil
import unittest
import pkg_resources


from pyparade.util.btree import BTree, BTreeFileLeafFactory

class TestBTree(unittest.TestCase):
	def setUp(self):
		self.btree = BTree(3, int, int)

	def tearDown(self):
		pass

	def test_insert(self):
		self.btree.insert(1,101)
		self.assertEqual(1, self.btree.root.childs[0].keys[0])
		self.assertEqual(101, self.btree.root.childs[0].values[0])

		self.btree.insert(2,102)
		self.assertEqual(1, self.btree.root.childs[0].keys[0])
		self.assertEqual(101, self.btree.root.childs[0].values[0])
		self.assertEqual(2, self.btree.root.childs[0].keys[1])
		self.assertEqual(102, self.btree.root.childs[0].values[1])

		self.btree.insert(3,103)
		self.assertEqual([1,2,3], self.btree.root.childs[0].keys)
		self.assertEqual([101,102,103], self.btree.root.childs[0].values)

		self.btree.insert(4,104)
		self.assertEqual([1,2], self.btree.root.childs[0].keys)
		self.assertEqual([101,102], self.btree.root.childs[0].values)
		self.assertEqual([3,4], self.btree.root.childs[1].keys)
		self.assertEqual([103,104], self.btree.root.childs[1].values)
		self.assertEqual([3], self.btree.root.keys)

		numbers = [x for x in range(5,100)]
		random.shuffle(numbers)

		for number in numbers:
			self.btree.insert(number, 100+number)

		for number in numbers:
			self.assertIn(number, self.btree.root.search(number).keys)
			self.assertIn(100+number, self.btree.root.search(number).values)

	def test_remove(self):
		#insert
		numbers = [x for x in range(1,1000)]
		#numbers = [4, 13, 2, 12, 9, 10, 8, 1, 6, 5, 3, 7, 11, 14] 
		random.shuffle(numbers)

		for number in numbers:
			self.btree.insert(number, 100+number)

		#test delete
		random.shuffle(numbers)

		#numbers = [10,6,14,12,9,4,5,3,1,13,8,11,2,7]
		for number in numbers:
			self.assertIn(number, self.btree.root.search(number).keys)
			self.assertIn(100+number, self.btree.root.search(number).values)
			self.btree.remove(number)
			self.assertNotIn(number, self.btree.root.search(number).keys)
			self.assertNotIn(100+number, self.btree.root.search(number).values)

		self.assertEqual(0, len(self.btree.root))
		self.assertEqual([], self.btree.root.childs[0].keys)
		self.assertEqual([], self.btree.root.childs[0].values)

@total_ordering
class IntClass(object):
	def __init__(self, integer):
		self.integer = integer

	def to_JSON(self):
		return self.integer

	@staticmethod
	def from_JSON(json):
		return IntClass(json)

	def __str__(self):
		return str(self.integer)

	def __lt__(self, other):
		return self.integer < other.integer

	def __eq__(self, other):
		return (isinstance(other, self.__class__)) and (self.integer == other.integer)

class TestBTreeFileLeaves(unittest.TestCase):
	def setUp(self):
		self.tempdir = tempfile.mkdtemp()
		self.leaffactory = BTreeFileLeafFactory(os.path.join(self.tempdir, "page"), ".index")
		self.btree = BTree(50, IntClass, IntClass, self.leaffactory)
	
	def tearDown(self):
		shutil.rmtree(self.tempdir)
		pass

	def test_insert(self):
		numbers = [x for x in range(1,1000)]
		random.shuffle(numbers)

		for number in numbers:
			self.btree.insert(IntClass(number), IntClass(100+number))
			self.assertIn(number, [key.integer for key in self.btree.root.search(IntClass(number)).keys])
			self.assertIn(100+number, [value.integer for value in self.btree.root.search(IntClass(number)).values])
			self.leaffactory.cleanup()
		self.assertGreater(len(self.leaffactory.allocated_leaves),2)

	def test_remove(self):
		numbers = [x for x in range(1,1000)]
		random.shuffle(numbers)
		#insert
		for number in numbers:
			self.btree.insert(IntClass(number), IntClass(100+number))
			self.assertIn(number, [key.integer for key in self.btree.root.search(IntClass(number)).keys])
			self.assertIn(100+number, [value.integer for value in self.btree.root.search(IntClass(number)).values])

		#test delete
		random.shuffle(numbers)
		for number in numbers:
			leaf = self.btree.root.search(IntClass(number))
			leaf.ensure_load()
			self.assertIn(number, [key.integer for key in self.btree.root.search(IntClass(number)).keys])
			self.assertIn(100+number, [value.integer for value in self.btree.root.search(IntClass(number)).values])

			self.btree.remove(IntClass(number))

			leaf = self.btree.root.search(IntClass(number))
			leaf.ensure_load()
			self.assertNotIn(number, [key.integer for key in self.btree.root.search(IntClass(number)).keys])
			self.assertNotIn(100+number, [value.integer for value in self.btree.root.search(IntClass(number)).values])
			self.leaffactory.cleanup()

		self.assertEqual(0, len(self.btree.root))
		self.assertEqual([], self.btree.root.childs[0].keys)
		self.assertEqual([], self.btree.root.childs[0].values)
		self.assertEqual(1, len(self.leaffactory.allocated_leaves))

	def test_persistent(self):
		#load with numbers
		numbers = [x for x in range(1,1000)]
		random.shuffle(numbers)

		for number in numbers:
			self.btree.insert(IntClass(number), IntClass(100+number))

		#serialize and deserialize again
		treejson = self.btree.to_JSON()
		self.leaffactory = BTreeFileLeafFactory(os.path.join(self.tempdir, "page"), ".index")
		self.btree = BTree.from_JSON(treejson, IntClass, IntClass, self.leaffactory)

		#check for numbers
		for number in numbers:
			leaf = self.btree.root.search(IntClass(number))
			leaf.ensure_load()
			self.assertIn(number, [key.integer for key in leaf.keys])

		self.assertGreater(len(self.leaffactory.allocated_leaves),2)

		#test delete to check if parent correctly set on all nodes and tree works
		random.shuffle(numbers)
		for number in numbers:
			leaf = self.btree.root.search(IntClass(number))
			leaf.ensure_load()
			self.assertIn(number, [key.integer for key in self.btree.root.search(IntClass(number)).keys])
			self.assertIn(100+number, [value.integer for value in self.btree.root.search(IntClass(number)).values])

			self.btree.remove(IntClass(number))

			leaf = self.btree.root.search(IntClass(number))
			leaf.ensure_load()
			self.assertNotIn(number, [key.integer for key in self.btree.root.search(IntClass(number)).keys])
			self.assertNotIn(100+number, [value.integer for value in self.btree.root.search(IntClass(number)).values])
			self.leaffactory.cleanup()

		self.assertEqual(0, len(self.btree.root))
		self.assertEqual([], self.btree.root.childs[0].keys)
		self.assertEqual([], self.btree.root.childs[0].values)
		self.assertEqual(1, len(self.leaffactory.allocated_leaves))

