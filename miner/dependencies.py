from miner.config import WorkerConfig, factory_worker_config


def get_worker_config() -> WorkerConfig:
    return factory_worker_config()
