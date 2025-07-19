import asyncio
import logging
from pyrogram import Client

class PremiumSessionPool:
    def __init__(self, session_string, max_sessions=3):
        self.session_string = session_string
        self.max_sessions = max_sessions
        self.sessions = []
        self.session_semaphore = asyncio.Semaphore(max_sessions)
        self.session_locks = {}  # Lock per session to prevent concurrent use
        self.current_session_index = 0
        self.logger = logging.getLogger("PremiumSessionPool")

    async def  soget_session(self):
        async with self.session_semaphore:
            if not self.sessions:
                # Initialize first session
                return await self._create_new_session()
            
            # Try to get an available session
            for _ in range(len(self.sessions)):
                session = self.sessions[self.current_session_index]
                session_lock = self.session_locks.get(id(session))
                
                if session_lock is None or not session_lock.locked():
                    # Create a new lock if doesn't exist
                    if session_lock is None:
                        self.session_locks[id(session)] = asyncio.Lock()
                    
                    # Try to acquire the lock
                    if await self.session_locks[id(session)].acquire():
                        return session
                
                # Move to next session
                self.current_session_index = (self.current_session_index + 1) % len(self.sessions)
            
            # If no available session, create new one if under limit
            if len(self.sessions) < self.max_sessions:
                return await self._create_new_session()
            
            # Wait for an available session
            while True:
                session = self.sessions[self.current_session_index]
                session_lock = self.session_locks[id(session)]
                
                if not session_lock.locked():
                    await session_lock.acquire()
                    return session
                
                self.current_session_index = (self.current_session_index + 1) % len(self.sessions)
                await asyncio.sleep(1)

    async def _create_new_session(self):
        try:
            session = Client(f"premium_bot_{len(self.sessions)}", session_string=self.session_string)
            await session.start()
            self.sessions.append(session)
            self.session_locks[id(session)] = asyncio.Lock()
            await self.session_locks[id(session)].acquire()
            self.logger.info(f"Created new premium session {len(self.sessions)}")
            return session
        except Exception as e:
            self.logger.error(f"Failed to create premium session: {e}")
            raise

    async def release_session(self, session):
        if session in self.sessions:
            session_lock = self.session_locks.get(id(session))
            if session_lock and session_lock.locked():
                session_lock.release()

    async def close_all_sessions(self):
        for session in self.sessions:
            try:
                await session.stop()
            except Exception as e:
                self.logger.error(f"Error closing session: {e}")
        self.sessions.clear()
        self.session_locks.clear()

# Initialize the premium session pool
