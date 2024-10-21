# Table names
API_KEYS_TABLE = "api_keys"
LOGS_TABLE = "logs"
HOTKEY_INFO_TABLE = "hotkey_info"
NODES_TABLE = "nodes"
NODES_HISTORY_TABLE = "nodes_history"
CONTENDERS_TABLE = "contenders"
CONTENDERS_HISTORY_TABLE = "contenders_history"

CONTENDERS_WEIGHTS_STATS_TABLE = "contenders_weights_stats"
NODES_WEIGHTS_TABLE = "nodes_weights"

TABLE_TASKS = "tasks"
TABLE_REWARD_DATA = "reward_data"
TABLE_UID_RECORDS = "uid_records"

DELAY_BETWEEN_SYNTHETIC_REQUESTS = "delay_between_synthetic_requests"
SYNTHETIC_REQUESTS_STILL_TO_MAKE = "synthetic_requests_still_to_make"

######## Column names ###########

# Api key table and logs table
KEY = "key"
NAME = "name"
BALANCE = "balance"
RATE_LIMIT_PER_MINUTE = "rate_limit_per_minute"
ENDPOINT = "endpoint"
COST = "cost"
CREATED_AT = "created_at"



# Hotkey info table
HOTKEY = "hotkey"
NODE_IP = "NODE_ip"
UID = "uid"

# NODE info table
HOTKEY = "hotkey"
COLDKEY = "coldkey"
NODE_ID = "node_id"
INCENTIVE = "incentive"
NETUID = "netuid"
STAKE = "stake"
TRUST = "trust"
VTRUST = "vtrust"
LAST_UPDATED = "last_updated"
IP = "ip"
IP_TYPE = "ip_type"
PORT = "port"
PROTOCOL = "protocol"
NETWORK = "network"
SYMMETRIC_KEY = "symmetric_key"
SYMMETRIC_KEY_UUID = "symmetric_key_uuid"
OUR_VALIDATOR = "our_validator"
CREATED_AT = "created_at"


# NODE Info history table

# HOTKEY = "hotkey"
# COLDKEY = "coldkey"
# NODE_ID = "node_id"
# INCENTIVE = "incentive"
# NETUID = "netuid"
# STAKE = "stake"
# TRUST = "trust"
# VTRUST = "vtrust"
# IP = "ip"
# IP_TYPE = "ip_type"
# PORT = "port"
# PROTOCOL = "protocol"
# NETWORK = "network"
# SYMMETRIC_KEY = "symmetric_key"
# SYMMETRIC_KEY_UUID = "symmetric_key_uuid"
# CREATED_AT = "created_at"
# OUR_VALIDATOR = "our_validator"
EXPIRED_AT = "expired_at"

# Contenders table
CONTENDER_ID = "contender_id"
NODE_HOTKEY = "node_hotkey"
NODE_ID = "node_id"
TASK = "task"
VALIDATOR_HOTKEY = "validator_hotkey"
RAW_CAPACITY = "raw_capacity"
CAPACITY = "capacity"
CAPACITY_TO_SCORE = "capacity_to_score"
CONSUMED_CAPACITY = "consumed_capacity"

TOTAL_REQUESTS_MADE = "total_requests_made"
REQUESTS_429 = "requests_429"
REQUESTS_500 = "requests_500"
PERIOD_SCORE = "period_score"
CREATED_AT = "created_at"
UPDATED_AT = "updated_at"

# Contenders history table

# CONTENDER_ID = "contender_id"
# MINER_HOTKEY = "node_hotkey"
# TASK = "task"
# VALIDATOR_HOTKEY = "validator_hotkey"

# RAW_CAPACITY = "raw_capacity"
# CAPACITY = "capacity"
# CAPACITY_TO_SCORE = "capacity_to_score"
# CONSUMED_CAPACITY = "consumed_capacity"

# DELAY_BETWEEN_SYNTHETIC_REQUESTS = "delay_between_synthetic_requests"
# SYNTHETIC_REQUESTS_STILL_TO_MAKE = "synthetic_requests_still_to_make"
# TOTAL_REQUESTS_MADE = "total_requests_made"
# REQUESTS_429 = "requests_429"
# REQUESTS_500 = "requests_500"
# PERIOD_SCORE = "period_score"
# CREATED_AT = "created_at"
# UPDATED_AT = "updated_at"
EXPIRED_AT = "expired_at"


# Common column names
COLUMN_ID = "id"
COLUMN_CREATED_AT = "created_at"
COLUMN_MINER_HOTKEY = "node_hotkey"

# `tasks` table column names
COLUMN_TASK_NAME = "task_name"
COLUMN_CHECKING_DATA = "checking_data"

# `reward_data` table column names
COLUMN_TASK = "task"
COLUMN_NODE_ID = "node_id"
COLUMN_QUALITY_SCORE = "quality_score"
COLUMN_VALIDATOR_HOTKEY = "validator_hotkey"
COLUMN_SYNTHETIC_QUERY = "synthetic_query"
COLUMN_METRIC = "metric"
COLUMN_RESPONSE_TIME = "response_time"
COLUMN_VOLUME = "volume"

# UID record columns
COLUMN_DECLARED_VOLUME = "declared_volume"
COLUMN_CONSUMED_VOLUME = "consumed_volume"
COLUMN_TOTAL_REQUESTS_MADE = "total_requests_made"
COLUMN_REQUESTS_429 = "requests_429"
COLUMN_REQUESTS_500 = "requests_500"
COLUMN_PERIOD_SCORE = "period_score"


# Weights and scoring stats
VERSION_KEY = "version_key"
COLUMN_WEIGHT = "node_weight"
COLUMN_AVERAGE_QUALITY_SCORE = "average_quality_score"
COLUMN_METRIC_BONUS = "metric_bonus"
COLUMN_COMBINED_QUALITY_SCORE = "combined_quality_score"
COLUMN_PERIOD_SCORE_MULTIPLIER = "period_score_multiplier"
COLUMN_NORMALISED_PERIOD_SCORE = "normalised_period_score"
COLUMN_CONTENDER_CAPACITY = "contender_capacity"
COLUMN_NORMALISED_NET_SCORE = "normalised_net_score"