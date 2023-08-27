import pymongo
import time

class LockDetails:
    def __init__(self, owner: str = "", host: str = "", comment: str = "", ttl: int = 0):
        self.owner = owner
        self.host = host
        self.comment = comment
        self.ttl = ttl

class Lock:
    def __init__(self, db, collection, createOptioanlIndex: bool = False):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = db
        self.collection = collection
        self.collection.create_index('resource', unique = True, background = True, sparse = True)
        if createOptioanlIndex:
            self.create_index()
        
  
    def create_index(self):
        self.collection.create_index("exclusive.lockId")
        self.collection.create_index("exclusive.expiresAt")
        self.collection.create_index("shared.locks.lockId")
        self.collection.create_index("shared.locks.expiresAt")

    def accquire_exclusive_lock(self, resource_name: str, lock_id: str, lock_details: LockDetails) -> bool:
        current_time = time.time()
        try:
            lock = self.collection.find_one_and_update(
                {
                    "resource": resource_name,
                    "$or": [
                        {"exclusive.acquired": False},
                        {"exclusive.expiresAt": {"$lt": current_time}}
                    ]
                },
                {
                    "$set": {
                        "resource": resource_name,
                        "exclusive": self.lock_from_details(lock_id, lock_details),
                        "shared.count": 0
                    }
                },
                upsert=True,
                return_document=pymongo.ReturnDocument.AFTER
            )
            
            if lock:
                return True

        except Exception as e:
            print(e)

        return False
    
    def release_exclusive_lock(self, resource_name: str, lock_id: str) -> bool:
        try:
            result = self.collection.update_one(
                {
                    "resource": resource_name,
                    "exclusive.lockId": lock_id
                },
                {
                    "$set": {
                        "exclusive": {
                            "acquired": False
                        }
                    }
                }
            )
            
            if result.modified_count == 1:
                return True

        except Exception as e:
            print(e)

        return False        
        
    
    def lock_from_details(self, lock_id: str, lock_details: LockDetails):
        now = time.time()
        
        lock = {
            "lockId": lock_id,
            "createdAt": now,
            "acquired": True,
        }
        
        if lock_details.owner:
            lock["owner"] = lock_details.owner
        
        if lock_details.host:
            lock["host"] = lock_details.host
            
        if lock_details.comment:
            lock["comment"] = lock_details.comment
            
        if lock_details.ttl > 0:
            lock["expiresAt"] = now + lock_details.ttl
            
        return lock
              

class LockStatus:
    def __init__(self, resource, lockId, type, owner, host, comment, createdAt, renewedAt, ttl, objectID):
        self.resource = resource
        self.lockId = lockId
        self.type = type
        self.owner = owner
        self.host = host
        self.comment = comment
        self.createdAt = createdAt
        self.renewedAt = renewedAt
        self.ttl = ttl
        self.objectId = objectID
        
        







