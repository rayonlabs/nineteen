"""
An example of what your custom tasks config file might look like.
Make a file called custom_task_config.py ( touch core/custom_task_config.py )
and add code similar to this to extend your task configs.
"""

from core.models import config_models as cmodels
from core.task_config import (
    CHAT_LLAMA_3_2_3B,
    DREAMSHAPER_IMAGE_TO_IMAGE,
    task_configs_factory,
)
from core.utils import get_updated_task_config_with_voted_weights

CHAT_LLAMA_3_1_405B = "chat-llama-3-1-405b"
ANIMAGINEXL_TEXT_TO_IMAGE = "animaginexl-text-to-image"


def custom_task_configs_factory():
    # This uses all the standard configs as base.
    base_task_config = task_configs_factory()
    # Except i might decide that I'm not interested now in the 8b model
    del base_task_config[CHAT_LLAMA_3_2_3B]
    # In fact, I might even feel like Dreamshaper ain't shaping the dreams enough
    del base_task_config[DREAMSHAPER_IMAGE_TO_IMAGE]

    # Now I will add the models I do want
    addition = {
        CHAT_LLAMA_3_1_405B: cmodels.FullTaskConfig(
            task=CHAT_LLAMA_3_1_405B,
            task_type=cmodels.TaskType.TEXT,
            max_capacity=4_000_000,  # I can also change the capacity however I like
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.LLM,
                load_model_config={
                    "model": "hugging-quants/Meta-Llama-3.1-405B-Instruct-GPTQ-INT4",  # This should exist on hugging face else it wont work
                    "half_precision": True,
                    "tokenizer": "llama-tokenizer-fix",  # This should be the tokenizer you like. Usually the base tokenizer, except for Llama's
                    "num_gpus": 4,
                    "tensor_parallel_size": 4,
                    "max_model_len": 16_000,
                    "gpu_utilization": 0.8,
                },
                endpoint=cmodels.Endpoints.chat_completions.value,
                checking_function="check_text_result",
                task=CHAT_LLAMA_3_1_405B,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_chat_synthetic", kwargs={"model": CHAT_LLAMA_3_1_405B}
            ),
            endpoint=cmodels.Endpoints.chat_completions.value,
            volume_to_requests_conversion=300,
            is_stream=True,
            weight=0.20,
            timeout=2,
            enabled=True,
        ),
        ANIMAGINEXL_TEXT_TO_IMAGE: cmodels.FullTaskConfig(
            task=ANIMAGINEXL_TEXT_TO_IMAGE,
            task_type=cmodels.TaskType.IMAGE,
            max_capacity=3_600,
            orchestrator_server_config=cmodels.OrchestratorServerConfig(
                server_needed=cmodels.ServerType.IMAGE,
                load_model_config={
                    "model_repo": "cagliostrolab/animagine-xl-3.1",  # This repo should exist on hugging face else it wont work
                    "safetensors_filename": "animagine-xl-3.1.safetensors",  # This safetensors model file should exist within the HF repo on the top level
                },
                checking_function="check_image_result",
                endpoint=cmodels.Endpoints.text_to_image.value,  # Currently supports SDXL finetunes etc, support for custom Flux and custom image-to-image soon!
                task=ANIMAGINEXL_TEXT_TO_IMAGE,
            ),
            synthetic_generation_config=cmodels.SyntheticGenerationConfig(
                func="generate_text_to_image_synthetic",
                kwargs={"model": ANIMAGINEXL_TEXT_TO_IMAGE},
            ),
            endpoint=cmodels.Endpoints.text_to_image.value,
            volume_to_requests_conversion=10,
            is_stream=False,
            weight=0.1,
            timeout=5,
            enabled=True,
        ),
    }

    combined_config = base_task_config | addition
    # Optionally use the voted weights to help set consensus
    combined_config = get_updated_task_config_with_voted_weights(combined_config)
    return combined_config
