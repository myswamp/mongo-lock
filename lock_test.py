import unittest
from lock import Lock, LockDetails
from testcontainers.mongodb import MongoDbContainer
import time

class TestStringMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("starting mongodb container...")
        cls.mongodb_container = MongoDbContainer("mongo:latest")
        cls.mongodb_container.start()
        cls.client = cls.mongodb_container.get_connection_client()
        cls.db = cls.client.test
        cls.collection = cls.db["mongolock"]

    @classmethod
    def tearDownClass(cls):
        print("\nstopping mongodb container...")        
        cls.mongodb_container.stop()
        
    def tearDown(self):
        self.collection.delete_many({})
        
    
    def test_accuire_xlock_concurrently(self):
        lock = Lock(self.db, self.collection)
        lock_details = LockDetails(ttl=0)
        locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
        self.assertTrue(locked)
        locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
        self.assertFalse(locked)
            
    def test_accuire_xlock_with_ttl(self):      
        lock = Lock(self.db, self.collection)
        lock_details = LockDetails(ttl=5)
        locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
        self.assertTrue(locked)
        
        time.sleep(5)
        
        locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
        self.assertTrue(locked)

    def test_release_xlock(self):
        lock = Lock(self.db, self.collection)
        lock_details = LockDetails(ttl=5)
        locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
        self.assertTrue(locked)
        
        unlocked = lock.release_exclusive_lock("resource", "lockid")
        self.assertTrue(unlocked)
        
        # realease an inexistent lock
        unlocked = lock.release_exclusive_lock("resource", "lockid2")
        self.assertFalse(unlocked)
            