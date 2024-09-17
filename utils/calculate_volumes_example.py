"""
Use this code to calculate a volume estimation for each task
If you prefer, you can remove the 'mean' estimation and instead insert your own estimation of speed per step or token
So you can more accurately calculate your own volume
"""

from core.tasks import Task
from core import constants as ccst
from core.tasks_config import TaskType
from core import tasks_config


def calculate_volume_for_task(
    task: Task, concurrent_requests_each_gpu_server_can_handle: float, gpus_with_server_on: float = 1
) -> int:
    vol_in_seconds = gpus_with_server_on * concurrent_requests_each_gpu_server_can_handle * ccst.SCORING_PERIOD_TIME

    scoring_config = tasks_config.TASK_TO_CONFIG[task].scoring_config

    # You can change this if you have better/worse hardware
    mean = scoring_config.mean

    # In the case of llm, this will get the number of characters we can do
    if scoring_config.task_type == TaskType.TEXT:
        vol_in_chars = vol_in_seconds / mean
        vol_in_tokens = vol_in_chars / 4
        vol = vol_in_tokens
    elif scoring_config.task_type == TaskType.IMAGE:
        vol_in_steps = vol_in_seconds / mean
        vol = vol_in_steps
    elif scoring_config.task_type == TaskType.CLIP:
        vol_in_images = vol_in_seconds / mean
        vol = vol_in_images
    else:
        raise ValueError(f"Invalid task type: {scoring_config.task_type}")

    ## If we were to have average speed, we would have a volume in

    return int(vol)


# Here are some examples
calculate_volume_for_task(Task.chat_llama_3_1_8b, concurrent_requests_each_gpu_server_can_handle=20, gpus_with_server_on=1)

calculate_volume_for_task(Task.chat_llama_3_1_70b, concurrent_requests_each_gpu_server_can_handle=20, gpus_with_server_on=1)
calculate_volume_for_task(Task.proteus_text_to_image, concurrent_requests_each_gpu_server_can_handle=10, gpus_with_server_on=1)

calculate_volume_for_task(Task.proteus_image_to_image, concurrent_requests_each_gpu_server_can_handle=1, gpus_with_server_on=1)
