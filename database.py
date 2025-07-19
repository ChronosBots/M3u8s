import motor.motor_asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging
from bson import ObjectId

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, database_url: str, database_name: str = "pyrogram_bot"):
        """
        Initialize MongoDB connection
        
        Args:
            database_url: MongoDB connection string
            database_name: Name of the database
        """
        self.client = motor.motor_asyncio.AsyncIOMotorClient(database_url)
        self.db = self.client[database_name]
        
        # Collections
        self.users = self.db.users
        self.admins = self.db.admins
        self.banned_users = self.db.banned_users
        self.paid_users = self.db.paid_users
        self.auth_users = self.db.auth_users
        self.broadcast_logs = self.db.broadcast_logs
        self.settings = self.db.settings
        
    async def close(self):
        """Close database connection"""
        self.client.close()
        
    # ============ USER MANAGEMENT ============
    
    async def add_user(self, user_id: int, username: str = None, first_name: str = None, 
                      last_name: str = None, phone: str = None) -> bool:
        """Add a new user to the database"""
        try:
            user_data = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "phone": phone,
                "joined_date": datetime.now(),
                "last_active": datetime.now(),
                "is_active": True
            }
            
            result = await self.users.update_one(
                {"user_id": user_id},
                {"$set": user_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")
            return False
            
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user information"""
        try:
            return await self.users.find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None
            
    async def update_user_activity(self, user_id: int) -> bool:
        """Update user's last activity timestamp"""
        try:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {"last_active": datetime.now()}}
            )
            return True
        except Exception as e:
            logger.error(f"Error updating user activity {user_id}: {e}")
            return False
            
    async def get_all_users(self) -> List[Dict]:
        """Get all users"""
        try:
            cursor = self.users.find({})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []
            
    async def get_user_count(self) -> int:
        """Get total user count"""
        try:
            return await self.users.count_documents({})
        except Exception as e:
            logger.error(f"Error getting user count: {e}")
            return 0
            
    # ============ ADMIN MANAGEMENT ============
    
    async def add_admin(self, user_id: int, added_by: int = None, permissions: List[str] = None) -> bool:
        """Add a new admin"""
        try:
            if permissions is None:
                permissions = ["ban", "unban", "broadcast", "stats"]
                
            admin_data = {
                "user_id": user_id,
                "added_by": added_by,
                "added_date": datetime.now(),
                "permissions": permissions,
                "is_active": True
            }
            
            result = await self.admins.update_one(
                {"user_id": user_id},
                {"$set": admin_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding admin {user_id}: {e}")
            return False
            
    async def remove_admin(self, user_id: int) -> bool:
        """Remove admin privileges"""
        try:
            result = await self.admins.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error removing admin {user_id}: {e}")
            return False
            
    async def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        try:
            admin = await self.admins.find_one({"user_id": user_id, "is_active": True})
            return admin is not None
        except Exception as e:
            logger.error(f"Error checking admin status {user_id}: {e}")
            return False
            
    async def get_admin_permissions(self, user_id: int) -> List[str]:
        """Get admin permissions"""
        try:
            admin = await self.admins.find_one({"user_id": user_id, "is_active": True})
            return admin.get("permissions", []) if admin else []
        except Exception as e:
            logger.error(f"Error getting admin permissions {user_id}: {e}")
            return []
            
    async def get_all_admins(self) -> List[Dict]:
        """Get all admins"""
        try:
            cursor = self.admins.find({"is_active": True})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting all admins: {e}")
            return []
            
    # ============ BAN MANAGEMENT ============
    
    async def ban_user(self, user_id: int, banned_by: int, reason: str = None, 
                      duration: int = None) -> bool:
        """Ban a user"""
        try:
            ban_data = {
                "user_id": user_id,
                "banned_by": banned_by,
                "banned_date": datetime.now(),
                "reason": reason,
                "duration": duration,  # Duration in hours, None for permanent
                "is_active": True
            }
            
            if duration:
                ban_data["expires_at"] = datetime.now() + timedelta(hours=duration)
            
            result = await self.banned_users.update_one(
                {"user_id": user_id},
                {"$set": ban_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error banning user {user_id}: {e}")
            return False
            
    async def unban_user(self, user_id: int) -> bool:
        """Unban a user"""
        try:
            result = await self.banned_users.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": False, "unbanned_date": datetime.now()}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error unbanning user {user_id}: {e}")
            return False
            
    async def is_banned(self, user_id: int) -> bool:
        """Check if user is banned"""
        try:
            ban = await self.banned_users.find_one({"user_id": user_id, "is_active": True})
            if not ban:
                return False
                
            # Check if temporary ban has expired
            if ban.get("expires_at") and ban["expires_at"] < datetime.now():
                await self.unban_user(user_id)
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error checking ban status {user_id}: {e}")
            return False
            
    async def get_ban_info(self, user_id: int) -> Optional[Dict]:
        """Get ban information"""
        try:
            return await self.banned_users.find_one({"user_id": user_id, "is_active": True})
        except Exception as e:
            logger.error(f"Error getting ban info {user_id}: {e}")
            return None
            
    async def get_all_banned_users(self) -> List[Dict]:
        """Get all banned users"""
        try:
            cursor = self.banned_users.find({"is_active": True})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting banned users: {e}")
            return []
            
    # ============ PAID USER MANAGEMENT ============
    
    async def add_paid_user(self, user_id: int, plan: str, duration: int, 
                           amount: float, added_by: int = None) -> bool:
        """Add a paid user"""
        try:
            paid_data = {
                "user_id": user_id,
                "plan": plan,
                "amount": amount,
                "added_by": added_by,
                "start_date": datetime.now(),
                "end_date": datetime.now() + timedelta(days=duration),
                "duration_days": duration,
                "is_active": True,
                "auto_renew": False
            }
            
            result = await self.paid_users.update_one(
                {"user_id": user_id},
                {"$set": paid_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding paid user {user_id}: {e}")
            return False
            
    async def remove_paid_user(self, user_id: int) -> bool:
        """Remove paid user status"""
        try:
            result = await self.paid_users.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": False, "removed_date": datetime.now()}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error removing paid user {user_id}: {e}")
            return False
            
    async def is_paid_user(self, user_id: int) -> bool:
        """Check if user is paid"""
        try:
            paid = await self.paid_users.find_one({"user_id": user_id, "is_active": True})
            if not paid:
                return False
                
            # Check if subscription has expired
            if paid.get("end_date") and paid["end_date"] < datetime.now():
                await self.remove_paid_user(user_id)
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error checking paid status {user_id}: {e}")
            return False
            
    async def get_paid_user_info(self, user_id: int) -> Optional[Dict]:
        """Get paid user information"""
        try:
            return await self.paid_users.find_one({"user_id": user_id, "is_active": True})
        except Exception as e:
            logger.error(f"Error getting paid user info {user_id}: {e}")
            return None
            
    async def get_all_paid_users(self) -> List[Dict]:
        """Get all paid users"""
        try:
            cursor = self.paid_users.find({"is_active": True})
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting paid users: {e}")
            return []
            
    async def get_expired_paid_users(self) -> List[Dict]:
        """Get users whose subscription has expired"""
        try:
            cursor = self.paid_users.find({
                "is_active": True,
                "end_date": {"$lt": datetime.now()}
            })
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting expired paid users: {e}")
            return []
            
    # ============ AUTH MANAGEMENT ============
    
    async def add_auth_user(self, user_id: int, auth_level: str = "user", 
                           added_by: int = None) -> bool:
        """Add authorized user"""
        try:
            auth_data = {
                "user_id": user_id,
                "auth_level": auth_level,  # user, premium, vip, etc.
                "added_by": added_by,
                "auth_date": datetime.now(),
                "is_active": True
            }
            
            result = await self.auth_users.update_one(
                {"user_id": user_id},
                {"$set": auth_data},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding auth user {user_id}: {e}")
            return False
            
    async def remove_auth_user(self, user_id: int) -> bool:
        """Remove user authorization"""
        try:
            result = await self.auth_users.delete_one({"user_id": user_id})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error removing auth user {user_id}: {e}")
            return False
            
    async def is_auth_user(self, user_id: int) -> bool:
        """Check if user is authorized"""
        try:
            auth = await self.auth_users.find_one({"user_id": user_id, "is_active": True})
            return auth is not None
        except Exception as e:
            logger.error(f"Error checking auth status {user_id}: {e}")
            return False
            
    async def get_auth_level(self, user_id: int) -> Optional[str]:
        """Get user authorization level"""
        try:
            auth = await self.auth_users.find_one({"user_id": user_id, "is_active": True})
            return auth.get("auth_level") if auth else None
        except Exception as e:
            logger.error(f"Error getting auth level {user_id}: {e}")
            return None
            
    # ============ BROADCAST MANAGEMENT ============
    
    async def create_broadcast(self, message: str, sent_by: int, 
                             target_type: str = "all") -> str:
        """Create a broadcast entry"""
        try:
            broadcast_data = {
                "message": message,
                "sent_by": sent_by,
                "target_type": target_type,  # all, paid, auth, admins
                "created_date": datetime.now(),
                "status": "pending",
                "total_users": 0,
                "sent_count": 0,
                "failed_count": 0,
                "completed": False
            }
            
            result = await self.broadcast_logs.insert_one(broadcast_data)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error creating broadcast: {e}")
            return None
            
    async def update_broadcast_stats(self, broadcast_id: str, sent_count: int, 
                                   failed_count: int, completed: bool = False) -> bool:
        """Update broadcast statistics"""
        try:
            update_data = {
                "sent_count": sent_count,
                "failed_count": failed_count,
                "completed": completed
            }
            
            if completed:
                update_data["completed_date"] = datetime.now()
                
            result = await self.broadcast_logs.update_one(
                {"_id": ObjectId(broadcast_id)},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating broadcast stats: {e}")
            return False
            
    async def get_broadcast_targets(self, target_type: str) -> List[int]:
        """Get user IDs for broadcast based on target type"""
        try:
            user_ids = []
            
            if target_type == "all":
                cursor = self.users.find({"is_active": True}, {"user_id": 1})
                users = await cursor.to_list(length=None)
                user_ids = [user["user_id"] for user in users]
                
            elif target_type == "paid":
                cursor = self.paid_users.find({"is_active": True}, {"user_id": 1})
                users = await cursor.to_list(length=None)
                user_ids = [user["user_id"] for user in users]
                
            elif target_type == "auth":
                cursor = self.auth_users.find({"is_active": True}, {"user_id": 1})
                users = await cursor.to_list(length=None)
                user_ids = [user["user_id"] for user in users]
                
            elif target_type == "admins":
                cursor = self.admins.find({"is_active": True}, {"user_id": 1})
                users = await cursor.to_list(length=None)
                user_ids = [user["user_id"] for user in users]
                
            return user_ids
        except Exception as e:
            logger.error(f"Error getting broadcast targets: {e}")
            return []
            
    async def get_broadcast_history(self, limit: int = 10) -> List[Dict]:
        """Get broadcast history"""
        try:
            cursor = self.broadcast_logs.find().sort("created_date", -1).limit(limit)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Error getting broadcast history: {e}")
            return []
            
    # ============ STATISTICS ============
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get bot statistics"""
        try:
            stats = {
                "total_users": await self.get_user_count(),
                "total_admins": await self.admins.count_documents({"is_active": True}),
                "total_banned": await self.banned_users.count_documents({"is_active": True}),
                "total_paid": await self.paid_users.count_documents({"is_active": True}),
                "total_auth": await self.auth_users.count_documents({"is_active": True}),
                "active_users_24h": await self.users.count_documents({
                    "last_active": {"$gte": datetime.now() - timedelta(hours=24)}
                }),
                "active_users_7d": await self.users.count_documents({
                    "last_active": {"$gte": datetime.now() - timedelta(days=7)}
                }),
                "new_users_today": await self.users.count_documents({
                    "joined_date": {"$gte": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)}
                })
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
            
    # ============ SETTINGS ============
    
    async def get_setting(self, key: str, default=None):
        """Get a setting value"""
        try:
            setting = await self.settings.find_one({"key": key})
            return setting.get("value", default) if setting else default
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}")
            return default
            
    async def set_setting(self, key: str, value: Any) -> bool:
        """Set a setting value"""
        try:
            result = await self.settings.update_one(
                {"key": key},
                {"$set": {"key": key, "value": value, "updated_date": datetime.now()}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error setting {key}: {e}")
            return False
            
    # ============ MAINTENANCE ============
    
    async def cleanup_expired_data(self) -> Dict[str, int]:
        """Clean up expired bans and subscriptions"""
        try:
            cleanup_stats = {"expired_bans": 0, "expired_subscriptions": 0}
            
            # Clean expired bans
            current_time = datetime.now()
            expired_bans = await self.banned_users.update_many(
                {"expires_at": {"$lt": current_time}, "is_active": True},
                {"$set": {"is_active": False, "unbanned_date": current_time}}
            )
            cleanup_stats["expired_bans"] = expired_bans.modified_count
            
            # Clean expired subscriptions
            expired_subs = await self.paid_users.update_many(
                {"end_date": {"$lt": current_time}, "is_active": True},
                {"$set": {"is_active": False, "removed_date": current_time}}
            )
            cleanup_stats["expired_subscriptions"] = expired_subs.modified_count
            
            return cleanup_stats
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return {"expired_bans": 0, "expired_subscriptions": 0}
            
    async def create_indexes(self):
        """Create database indexes for better performance"""
        try:
            # Users indexes
            await self.users.create_index("user_id", unique=True)
            await self.users.create_index("last_active")
            await self.users.create_index("joined_date")
            
            # Admins indexes
            await self.admins.create_index("user_id", unique=True)
            
            # Banned users indexes
            await self.banned_users.create_index("user_id")
            await self.banned_users.create_index("expires_at")
            
            # Paid users indexes
            await self.paid_users.create_index("user_id")
            await self.paid_users.create_index("end_date")
            
            # Auth users indexes
            await self.auth_users.create_index("user_id", unique=True)
            
            # Broadcast logs indexes
            await self.broadcast_logs.create_index("created_date")
            
            # Settings indexes
            await self.settings.create_index("key", unique=True)
            
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
