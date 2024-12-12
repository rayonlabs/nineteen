from functools import lru_cache
import importlib
from fiber.logging_utils import get_logger

from core.utils import get_updated_task_config_with_voted_weights, normalise_task_config_weights
from core.models import config_models as cmodels
from core import constants as cst

logger = get_logger(__name__)


CHAT_LLAMA_3_2_3B = "chat-llama-3-2-3b"
CHAT_LLAMA_3_1_70B = "chat-llama-3-1-70b"
CHAT_LLAMA_3_1_8B = "chat-llama-3-1-8b"

CHAT_LLAMA_3_2_3B_COMP = "chat-llama-3-2-3b-comp"
CHAT_LLAMA_3_1_70B_COMP = "chat-llama-3-1-70b-comp"
CHAT_LLAMA_3_1_8B_COMP = "chat-llama-3-1-8b-comp"

CHAT_ROGUE_ROSE_103B_COMP = "chat-rogue-rose-103b-comp"

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
            display_name="Llama 3.2 3B",
            created=1733599299,
            description="Llama 3.2 3B is a finetune of [Llama 3.2 3B](/unsloth/llama-3.2-3b-instruct) with a \"HUGE step up dataset wise\" compared to Llama 3.1 8B. Sloppy chats output were purged.\n\nUsage of this model is subject to [Meta's Acceptable Use Policy](https://llama.meta.com/llama3/use-policy/).",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "unsloth/Llama-3.2-3B-Instruct",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 20_000,
                    "gpu_memory_utilization": 0.5,
                    "eos_token_id": 128009
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_2_3B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", kwargs={"model": CHAT_LLAMA_3_2_3B}
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.025,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": "llama3"
            }
        ),
        CHAT_LLAMA_3_2_3B_COMP: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_2_3B_COMP,
            display_name="Llama 3.2 3B Completions",
            description="Llama 3.2 3B is a finetune of [Llama 3.2 3B](/unsloth/llama-3.2-3b-instruct) with a \"HUGE step up dataset wise\" compared to Llama 3.1 8B. Sloppy chats output were purged.\n\nUsage of this model is subject to [Meta's Acceptable Use Policy](https://llama.meta.com/llama3/use-policy/).",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "unsloth/Llama-3.2-3B-Instruct",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 20_000,
                    "gpu_memory_utilization": 0.5,
                    "eos_token_id": 128009
                },
                endpoint=cmodels.Endpoints.completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_2_3B_COMP,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_comp_synthetic", kwargs={"model": CHAT_LLAMA_3_2_3B_COMP}
            ),
            endpoint=cmodels.Endpoints.completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.025,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": "llama3"
            }
        ),
        CHAT_LLAMA_3_1_70B: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_70B,
            display_name="Llama 3.1 70B",
            description="Llama 3.1 70B is a finetune of [Llama 3.1 70B](/hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4) with a \"HUGE step up dataset wise\" compared to Llama 3.1 8B. Sloppy chats output were purged.\n\nUsage of this model is subject to [Meta's Acceptable Use Policy](https://llama.meta.com/llama3/use-policy/).",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 16_000,
                    "gpu_memory_utilization": 0.57,
                    "eos_token_id": 128009
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_70B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", kwargs={"model": CHAT_LLAMA_3_1_70B}
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.10,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": "llama3"
            }
        ),
        CHAT_LLAMA_3_1_70B_COMP: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_70B_COMP,
            display_name="Llama 3.1 70B Completions",
            description="Llama 3.1 70B is a finetune of [Llama 3.1 70B](/hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4) with a \"HUGE step up dataset wise\" compared to Llama 3.1 8B. Sloppy chats output were purged.\n\nUsage of this model is subject to [Meta's Acceptable Use Policy](https://llama.meta.com/llama3/use-policy/).",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 16_000,
                    "gpu_memory_utilization": 0.57,
                    "eos_token_id": 128009
                },
                endpoint=cmodels.Endpoints.completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_70B_COMP,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_comp_synthetic", kwargs={"model": CHAT_LLAMA_3_1_70B_COMP}
            ),
            endpoint=cmodels.Endpoints.completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.10,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": "llama3"
            }
        ),
        CHAT_LLAMA_3_1_8B: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_8B,
            display_name="Llama 3.1 8B",
            description="Llama 3.1 8B is a finetune of [Llama 3.1 8B](/unsloth/Meta-Llama-3.1-8B-Instruct). Sloppy chats output were purged.\n\nUsage of this model is subject to [Meta's Acceptable Use Policy](https://llama.meta.com/llama3/use-policy/).",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "unsloth/Meta-Llama-3.1-8B-Instruct",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 20_000,
                    "gpu_memory_utilization": 0.5,
                    "eos_token_id": 128009
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_8B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", kwargs={"model": CHAT_LLAMA_3_1_8B}
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.075,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": "llama3"
            }
        ),
        CHAT_LLAMA_3_1_8B_COMP: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_8B_COMP,
            display_name="Llama 3.1 8B Completions",
            description="Llama 3.1 8B is a finetune of [Llama 3.1 8B](/unsloth/Meta-Llama-3.1-8B-Instruct). Sloppy chats output were purged.\n\nUsage of this model is subject to [Meta's Acceptable Use Policy](https://llama.meta.com/llama3/use-policy/).",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "unsloth/Meta-Llama-3.1-8B-Instruct",
                    "half_precision": True,
                    "tokenizer": "tau-vision/llama-tokenizer-fix",
                    "max_model_len": 20_000,
                    "gpu_memory_utilization": 0.5,
                    "eos_token_id": 128009
                },
                endpoint=cmodels.Endpoints.completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_8B_COMP,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_comp_synthetic", kwargs={"model": CHAT_LLAMA_3_1_8B_COMP}
            ),
            endpoint=cmodels.Endpoints.completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.075,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": "llama3"
            }
        ),
        CHAT_ROGUE_ROSE_103B_COMP: cmodels.FullTaskConfig(
            task=CHAT_ROGUE_ROSE_103B_COMP,
            display_name="Rogue Rose 103B",
            description="Rogue Rose 103B makes roleplay go brr",
            task_type=cmodels.TaskType.TEXT,
            max_capacity=60_000,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "TheBloke/Rogue-Rose-103b-v0.2-AWQ",
                    "tokenizer": "TheBloke/Rogue-Rose-103b-v0.2-AWQ",
                    "half_precision": True,
                    "max_model_len": 4096,
                    "gpu_memory_utilization": 0.8,
                    "eos_token_id": 2,
                },
                endpoint=cmodels.Endpoints.completions.value,
                task=CHAT_ROGUE_ROSE_103B_COMP,
                checking_function="check_text_result",
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(func="generate_chat_comp_synthetic", kwargs={"model": CHAT_ROGUE_ROSE_103B_COMP}),
            endpoint=cmodels.Endpoints.completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.10,
            timeout=2,
            enabled=True,
            architecture={
                "modality": "text->text",
                "instruct_type": None
            }
        ),
        PROTEUS_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=PROTEUS_TEXT_TO_IMAGE,
            display_name="Proteus Text to Image",
            description="Lightning fast high quality text to image model",
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
                kwargs={"model": PROTEUS_TEXT_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.1,
            timeout=5,
            enabled=True,
            model_info={"model": "dataautogpt3/ProteusV0.4-Lightning", cst.MIN_STEPS: 6, cst.MAX_STEPS: 12},
        ),
        PROTEUS_IMAGE_TO_IMAGE: cmodels.FullTaskConfig(
            task=PROTEUS_IMAGE_TO_IMAGE,
            display_name="Proteus Image to Image",
            description="Lightning fast high quality image to image model",
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
                kwargs={"model": PROTEUS_IMAGE_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.image_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=20,
            enabled=True,
            model_info={"model": "dataautogpt3/ProteusV0.4-Lightning", cst.MIN_STEPS: 6, cst.MAX_STEPS: 12},
        ),
        FLUX_SCHNELL_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=FLUX_SCHNELL_TEXT_TO_IMAGE,
            display_name="Flux Schnell Text to Image",
            description="Ultra high quality text to image model, capable of text",
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
                kwargs={"model": FLUX_SCHNELL_TEXT_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.10,
            timeout=20,
            enabled=True,
            model_info={"model": "black-forest-labs/FLUX.1-schnell", cst.MIN_STEPS: 2, cst.MAX_STEPS: 20},
        ),
        FLUX_SCHNELL_IMAGE_TO_IMAGE: cmodels.FullTaskConfig(
            task=FLUX_SCHNELL_IMAGE_TO_IMAGE,
            display_name="Flux Schnell Image to Image",
            description="Ultra high quality image to image model, capable of text",
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
                kwargs={"model": FLUX_SCHNELL_IMAGE_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.image_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=15,
            enabled=True,
            model_info={"model": "black-forest-labs/FLUX.1-schnell", cst.MIN_STEPS: 2, cst.MAX_STEPS: 20},
        ),
        AVATAR: cmodels.FullTaskConfig(
            task=AVATAR,
            display_name="Avatar",
            description="Make people into avatars",
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
                kwargs={},
            ),
            endpoint=cmodels.Endpoints.avatar.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.10,
            timeout=15,
            enabled=True,
            model_info={"model": "dataautogpt3/ProteusV0.4-Lightning"},
        ),
        DREAMSHAPER_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=DREAMSHAPER_TEXT_TO_IMAGE,
            display_name="Dreamshaper Text to Image",
            description="Ultra high quality text to image model, capable of text",
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
                kwargs={"model": DREAMSHAPER_TEXT_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=5,
            enabled=True,
            model_info={"model": "Lykon/dreamshaper-xl-lightning", cst.MIN_STEPS: 6, cst.MAX_STEPS: 12},
        ),
        DREAMSHAPER_IMAGE_TO_IMAGE: cmodels.FullTaskConfig(
            task=DREAMSHAPER_IMAGE_TO_IMAGE,
            display_name="Dreamshaper Image to Image",
            description="Ultra high quality image to image model, capable of image to image",
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
                kwargs={"model": DREAMSHAPER_IMAGE_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.image_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.05,
            timeout=15,
            enabled=True,
            model_info={"model": "Lykon/dreamshaper-xl-lightning", cst.MIN_STEPS: 6, cst.MAX_STEPS: 12},
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
