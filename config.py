from environs import Env


env = Env()
env.read_env()

# Postgresql variables.
PG_USER = env.str('PG_USER')
PG_PASS = env.str('PG_PASS')
PG_DATABASE = env.str('PG_DATABASE')
PG_HOST = env.str('PG_HOST')
PG_PORT = env.int('PG_PORT')

# Parsing variables.
PARSE_TIMEOUT = 120
PARSE_ATTEMPTS = 3
