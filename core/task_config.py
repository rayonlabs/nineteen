from functools import lru_cache
import importlib
from fiber.logging_utils import get_logger

from core.utils import get_updated_task_config_with_voted_weights, normalise_task_config_weights
from core.models import config_models as cmodels

logger = get_logger(__name__)


CHAT_LLAMA_3_2_3B = "chat-llama-3-2-3b"
CHAT_LLAMA_3_1_70B = "chat-llama-3-1-70b"
CHAT_LLAMA_3_1_8B = "chat-llama-3-1-8b"
PROTEUS_TEXT_TO_IMAGE = "proteus-text-to-image"
PROTEUS_IMAGE_TO_IMAGE = "proteus-image-to-image"
FLUX_SCHNELL_TEXT_TO_IMAGE = "flux-schnell-text-to-image"
FLUX_SCHNELL_IMAGE_TO_IMAGE = "flux-schnell-image-to-image"
AVATAR = "avatar"
DREAMSHAPER_TEXT_TO_IMAGE = "dreamshaper-text-to-image"
DREAMSHAPER_IMAGE_TO_IMAGE = "dreamshaper-image-to-image"

def task_configs_factory() -> dict[str, cmodels.FullTaskConfig]:
    return {
        CHAT_LLAMA_3_2_3B: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_2_3B,
            task_type=cmodels.TaskType.TEXT,
            max_capacity=120_000, 
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "unsloth/Llama-3.2-3B-Instruct",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 20_000,
                    "gpu_utilization": 0.65,
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_2_3B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", 
                kwargs={
                    "model": CHAT_LLAMA_3_2_3B,
                    "max_input_words": 140,
                    "max_output_tokens": 1024,
                    "max_temperature": 1.0
                }
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.05,
            timeout=2,
            enabled=True,
        ),
        CHAT_LLAMA_3_1_70B: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_70B,
            task_type=cmodels.TaskType.TEXT,
            max_capacity=120_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 16_000,
                    "gpu_utilization": 0.6,
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_70B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", 
                kwargs={
                    "model": CHAT_LLAMA_3_1_70B,
                    "max_input_words": 140,
                    "max_output_tokens": 1024,
                    "max_temperature": 1.0
                }
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.2,
            timeout=2,
            enabled=True,
        ),
        CHAT_LLAMA_3_1_8B: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_8B,
            task_type=cmodels.TaskType.TEXT,
            max_capacity=120_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "unsloth/Meta-Llama-3.1-8B-Instruct",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 20_000,
                    "gpu_utilization": 0.65,
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_8B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", 
                kwargs={
                    "model": CHAT_LLAMA_3_1_8B,
                    "max_input_words": 140,
                    "max_output_tokens": 1024,
                    "max_temperature": 1.0
                }
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.15,
            timeout=2,
            enabled=True,
        ),
        PROTEUS_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=PROTEUS_TEXT_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=800,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config = {},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.text_to_image.value,
                task=PROTEUS_TEXT_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_text_to_image_synthetic",
                kwargs={
                    "model": PROTEUS_TEXT_TO_IMAGE,
                    "max_prompt_words" : 20,
                    "height_range" : (400, 1600),
                    "width_range" : (400, 1600),
                    "cfg_scale_range" : (1.5, 3.0),
                    "steps_range" : (5, 10)
                },
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.1,
            timeout=5,
            enabled=True,
            model_info={"model": "dataautogpt3/ProteusV0.4-Lightning"},
        ),
        PROTEUS_IMAGE_TO_IMAGE: cmodels.FullTaskConfig(
            task=PROTEUS_IMAGE_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=800,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config= {},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.image_to_image.value,
                task=PROTEUS_IMAGE_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_image_to_image_synthetic",
                kwargs={
                    "model": PROTEUS_IMAGE_TO_IMAGE,
                    "max_prompt_words" : 20,
                    "height_range" : (400, 1600),
                    "width_range" : (400, 1600),
                    "cfg_scale_range" : (1.5, 3.0),
                    "steps_range" : (5, 10)
                },
            ),
            endpoint=cmodels.Endpoints.image_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=20,
            enabled=True,
            model_info={"model": "dataautogpt3/ProteusV0.4-Lightning"},
        ),
        FLUX_SCHNELL_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=FLUX_SCHNELL_TEXT_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=2100,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config={},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.text_to_image.value,
                task=FLUX_SCHNELL_TEXT_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_text_to_image_synthetic",
                kwargs={
                    "model": FLUX_SCHNELL_TEXT_TO_IMAGE,
                    "max_prompt_words" : 20,
                    "height_range" : (400, 1600),
                    "width_range" : (400, 1600),
                    "cfg_scale_range" : (1.0, 5.0),
                    "steps_range" : (2, 7)
                },
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.15,
            timeout=20,
            enabled=True,
            model_info={"model": "black-forest-labs/FLUX.1-schnell"},
        ),
        FLUX_SCHNELL_IMAGE_TO_IMAGE: cmodels.FullTaskConfig(
            task=FLUX_SCHNELL_IMAGE_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=800,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config={},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.image_to_image.value,
                task=FLUX_SCHNELL_IMAGE_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_image_to_image_synthetic",
                kwargs={
                    "model": FLUX_SCHNELL_IMAGE_TO_IMAGE,
                    "max_prompt_words" : 20,
                    "height_range" : (400, 1600),
                    "width_range" : (400, 1600),
                    "cfg_scale_range" : (1.0, 5.0),
                    "steps_range" : (2, 7)
                },
            ),
            endpoint=cmodels.Endpoints.image_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=15,
            enabled=True,
            model_info={"model": "black-forest-labs/FLUX.1-schnell"},
        ),
        AVATAR: cmodels.FullTaskConfig(
            task=AVATAR,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=800,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config={},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.avatar.value,
                task=AVATAR,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_avatar_synthetic",
                kwargs={
                    "max_prompt_words" : 20,
                    "steps_range" : (10, 20)
                },
            ),
            endpoint=cmodels.Endpoints.avatar.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.15,
            timeout=15,
            enabled=True,
            model_info={"model": "dataautogpt3/ProteusV0.4-Lightning"},
        ),
        DREAMSHAPER_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=DREAMSHAPER_TEXT_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=800,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config={},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.text_to_image.value,
                task=DREAMSHAPER_TEXT_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_text_to_image_synthetic",
                kwargs={
                    "model": DREAMSHAPER_TEXT_TO_IMAGE,
                    "max_prompt_words" : 20,
                    "height_range" : (400, 1600),
                    "width_range" : (400, 1600),
                    "cfg_scale_range" : (1.5, 3.0),
                    "steps_range" : (5, 10)
                },
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=5,
            enabled=True,
            model_info={"model": "Lykon/dreamshaper-xl-lightning"},
        ),
        DREAMSHAPER_IMAGE_TO_IMAGE: cmodels.FullTaskConfig(
            task=DREAMSHAPER_IMAGE_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=800,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config={},
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.image_to_image.value,
                task=DREAMSHAPER_IMAGE_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_image_to_image_synthetic",
                kwargs={
                    "model": DREAMSHAPER_IMAGE_TO_IMAGE,
                    "max_prompt_words" : 20,
                    "height_range" : (400, 1600),
                    "width_range" : (400, 1600),
                    "cfg_scale_range" : (1.5, 3.0),
                    "steps_range" : (5, 10)
                },
            ),
            endpoint=cmodels.Endpoints.image_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=15,
            enabled=True,
            model_info={"model": "Lykon/dreamshaper-xl-lightning"},
        ),
    }


logger.info("Fetching the latest weights...")


@lru_cache(maxsize=1)
def get_task_configs() -> dict[str, cmodels.FullTaskConfig]:
    """
    Get task configurations, with support for custom configs and caching.
    First try to find the custom config, else use the default one above.
    """
    try:
        custom_module = importlib.import_module("core.custom_task_config")
        if hasattr(custom_module, "custom_task_configs_factory"):
            logger.info("Loading custom task configurations factory.")
            task_configs = custom_module.custom_task_configs_factory()
            task_configs = normalise_task_config_weights(task_configs)
            return task_configs
        else:
            logger.warning(
                "custom_task_config.py found but custom_task_configs_factory not defined. Using default configuration."
            )
    except ImportError:
        logger.info("No custom_task_config.py found. Using default configuration.")
    except Exception as e:
        logger.error(f"Error loading custom_task_config.py: {e}. Using default configuration.")

    task_configs = task_configs_factory()
    logger.debug(f"Len of task configs: {len(task_configs)}")
    task_configs = get_updated_task_config_with_voted_weights(task_configs)
    logger.debug(f"Len of task configs after voting: {len(task_configs)}")
    task_configs = normalise_task_config_weights(task_configs)
    logger.debug(f"len of task configs after normalisation: {len(task_configs)}")
    return task_configs


def get_public_task_configs() -> list[dict]:
    task_configs = get_task_configs()
    return [config.get_public_config() for config in task_configs.values() if config.enabled]


def get_enabled_task_config(task: str) -> cmodels.FullTaskConfig | None:
    task_configs = get_task_configs()
    config = task_configs.get(task)
    if config is None or not config.enabled:
        return None
    return config
