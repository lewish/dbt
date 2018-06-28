import multiprocessing
import traceback


class ConnectionPool:
    def __init__(self):
        self.lock = multiprocessing.Lock()
        self.connections_in_use = {}
        self.connections_available = []


global_pool = ConnectionPool()


class ConnectionManager:
    connection_pools = {}

    @staticmethod
    def get_pool(profile):
        key = ConnectionManager.get_pool_key(profile)
        if key not in ConnectionManager.connection_pools.keys():
            ConnectionManager.connection_pools[key] = ConnectionPool()
        return ConnectionManager.connection_pools.get(key)

    @staticmethod
    def get_pool_key(profile):
        # Just return one big hash of all the known relevant profile properties, even though many are None.
        # The profile type appears to get lost in on certain code paths.
        return "|".join([str(value) if value else "" for value in [profile.get('project'),
                                                                   profile.get('host'), profile.get('port'),
                                                                   profile.get('user'), profile.get('dbname'),
                                                                   profile.get('account'), profile.get('database'),
                                                                   profile.get('warehouse'),
                                                                   profile.get('schema')]])
