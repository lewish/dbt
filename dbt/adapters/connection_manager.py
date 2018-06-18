import multiprocessing


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
        if profile.get('type') == 'bigquery':
            return 'bigquery|' + profile.get('project')
        elif profile.get('type') == 'redshift' or profile.get('type') == 'postgres':
            return 'postgres|{host}|{port}|{user}|{dbname}'.format(
                host=profile.get('host'), port=profile.get('port'),
                user=profile.get('user'),
                dbname={profile.get('dbname')})
        elif profile.get('type') == 'snowflake':
            return 'snowflake|{account}|{database}|{user}|{warehouse}|${schema}'.format(
                account=profile.get('account'),
                database=profile.get('database'),
                user=profile.get('user'),
                warehouse=profile.get('warehouse'),
                schema=profile.get('schema')
            )
        else:
            raise Exception('Invalid profile type: {}'.format(profile.get('type')))
