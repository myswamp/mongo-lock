import unittest
from lock import Lock, LockDetails
from testcontainers.mongodb import MongoDbContainer
import time

class TestStringMethods(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("\nsetUpClass method: Runs before all tests...")
    
    def test_accuire_xlock_concurrently(self):
        with MongoDbContainer("mongo:latest") as mongo:
            client = mongo.get_connection_client()
            db = client.test
            collection = db["mongolock"]
            
            lock = Lock(db, collection)
            lock_details = LockDetails(ttl=0)
            locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
            self.assertTrue(locked)
            locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
            self.assertFalse(locked)
            
    def test_accuire_xlock_with_ttl(self):
        with MongoDbContainer("mongo:latest") as mongo:
            client = mongo.get_connection_client()
            db = client.test
            collection = db["mongolock"]
            
            lock = Lock(db, collection)
            lock_details = LockDetails(ttl=5)
            locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
            self.assertTrue(locked)
            
            time.sleep(5)
            
            locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
            self.assertTrue(locked)

    def test_release_xlock(self):
        with MongoDbContainer("mongo:latest") as mongo:
            client = mongo.get_connection_client()
            db = client.test
            collection = db["mongolock"]
            
            lock = Lock(db, collection)
            lock_details = LockDetails(ttl=5)
            locked = lock.accquire_exclusive_lock("resource", "lockid", lock_details)
            self.assertTrue(locked)
            
            time.sleep(5)
            
            unlocked = lock.release_exclusive_lock("resource", "lockid")
            self.assertTrue(unlocked)
            
            # realease an inexistent lock
            unlocked = lock.release_exclusive_lock("resource", "lockid2")
            self.assertFalse(unlocked)
            
    @classmethod
    def tearDownClass(cls):
    	print("\ntearDownClass method: Runs after all tests...")

if __name__ == '__main__':
    unittest.main()