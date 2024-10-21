# Schema for the db


from core.task_config import get_task_configs
from core import constants as ccst
from validator.db.src import functions as db_functions
from validator.db.src.database import PSQLDB
from validator.db.src.sql.contenders import fetch_hotkey_scores_for_task
from validator.db.src.sql.weights import insert_scoring_stats, insert_weights, delete_weights_info_older_than, delete_miner_weights_older_than
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.utils.post.nineteen import DataTypeToPost, post_to_nineteen_ai, ContenderWeightsInfoPostObject, MinerWeightsPostObject
from validator.control_node.src.control_config import Config
from validator.db.src.sql.nodes import get_nodes
from validator.models import Contender, PeriodScore
from validator.models import RewardData
from datetime import datetime, timezone, timedelta
from fiber.logging_utils import get_logger


logger = get_logger(__name__)

PERIOD_SCORE_TIME_DECAYING_FACTOR = 0.5
METRIC_PERCENTILE = 0.3
SPEED_BONUS_MAX = 0.5


def _get_metric_score(metrics: list[float]) -> float:
    # get the METRIC_PERCENTILEth percentile
    metrics = sorted(metrics)
    percentile_index = int(len(metrics) * METRIC_PERCENTILE)
    percentile_value = metrics[percentile_index]

    return percentile_value


def _get_metric_bonuses(metric_scores: dict[str, float]) -> dict[str, float]:
    ranked_scores = {
        hotkey: rank for rank, (hotkey, _) in enumerate(sorted(metric_scores.items(), key=lambda x: x[1], reverse=True))
    }
    if len(ranked_scores) <= 1:
        return ranked_scores
    return {hotkey: SPEED_BONUS_MAX * (0.5 - rank / (len(metric_scores) - 1)) for hotkey, rank in ranked_scores.items()}


async def _get_reward_datas(psql_db: PSQLDB, task: str, netuid: int) -> list[RewardData]:
    # Flow is:
    # Get all possible hotkeys
    # Get reward data for this task - as much as possible
    # If there are not enough, then get the remaining from other tasks that hotkey has done

    all_nodes = await get_nodes(psql_db, netuid=netuid)
    all_hotkeys = [node.hotkey for node in all_nodes]
    reward_datas = []
    async with await psql_db.connection() as connection:
        for hotkey in all_hotkeys:
            reward_data = await db_functions.fetch_recent_most_rewards(connection, task, hotkey, quality_tasks_to_fetch=50)
            reward_datas.extend(reward_data)
    return reward_datas


async def _get_period_scores(psql_db: PSQLDB, task: str, node_hotkey: str) -> list[PeriodScore]:
    async with await psql_db.connection() as connection:
        period_scores = await fetch_hotkey_scores_for_task(connection, task, node_hotkey)
    return period_scores


async def _calculate_metrics_and_quality_score(psql_db: PSQLDB, task: str, netuid: int) -> tuple[dict[str, float], dict[str, float]]:
    reward_datas: list[RewardData] = await _get_reward_datas(psql_db, task, netuid)

    metrics = {}
    quality_scores = {}
    for reward_data in reward_datas:
        if reward_data.metric is None or reward_data.quality_score is None:
            logger.warning(
                f"Skipping reward data for task: {task} as metric or quality score is None"
                f" Metric: {reward_data.metric}, quality_score: {reward_data.quality_score}"
            )
            continue
        metrics[reward_data.node_hotkey] = metrics.get(reward_data.node_hotkey, []) + [reward_data.metric]
        quality_scores[reward_data.node_hotkey] = quality_scores.get(reward_data.node_hotkey, []) + [reward_data.quality_score]
    return metrics, quality_scores


async def _calculate_metric_bonuses(metrics: dict[str, float]) -> dict[str, float]:
    metric_scores = {node_hotkey: _get_metric_score(scores) for node_hotkey, scores in metrics.items()}
    metric_bonuses = _get_metric_bonuses(metric_scores)
    return metric_bonuses


async def _calculate_normalised_period_score(psql_db: PSQLDB, task: str, node_hotkey: str) -> tuple[float, float]:
    period_scores = await _get_period_scores(psql_db, task, node_hotkey)
    all_period_scores = [ps for ps in period_scores if ps.period_score is not None]
    # Requires an abundance of data before handing out top scores
    period_score_multiplier = 1 if len(all_period_scores) > 8 else 0.25
    normalised_period_scores = _normalise_period_scores(all_period_scores)
    return normalised_period_scores, period_score_multiplier


def _normalise_period_scores(period_scores: list[PeriodScore]) -> float:
    if len(period_scores) == 0:
        return 0

    sum_of_volumes = sum(ps.consumed_capacity for ps in period_scores)
    if sum_of_volumes == 0:
        return 0

    total_score = 0
    total_weight = 0
    for i, score in enumerate(period_scores):
        volume_weight = score.consumed_capacity / sum_of_volumes
        time_weight = (1 - PERIOD_SCORE_TIME_DECAYING_FACTOR) ** i
        combined_weight = volume_weight * time_weight
        if score.period_score is not None:
            total_score += score.period_score * combined_weight
            total_weight += combined_weight

    # Requires an abundance of data before handing out top scores
    period_score_multiplier = 1 if len(period_scores) > 8 else 0.25

    if total_weight == 0:
        return 0
    else:
        return period_score_multiplier * total_score / total_weight


def _calculate_hotkey_effective_volume_for_task(
    combined_quality_score: float, normalised_period_score: float, volume: float
) -> float:
    return combined_quality_score * normalised_period_score * volume

async def _process_quality_scores(psql_db: PSQLDB, task: str, netuid: int) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    metrics, quality_scores = await _calculate_metrics_and_quality_score(psql_db, task, netuid)
    average_weighted_quality_scores = {
        node_hotkey: sum(score**1.5 for score in scores) / len(scores) for node_hotkey, scores in quality_scores.items()
    }
    metric_bonuses = await _calculate_metric_bonuses(metrics)
    combined_quality_scores = {
        node_hotkey: average_weighted_quality_scores[node_hotkey] * (1 + metric_bonuses[node_hotkey]) for node_hotkey in metrics
    }
    return combined_quality_scores, average_weighted_quality_scores, metric_bonuses

async def _calculate_effective_volumes_for_task(psql_db: PSQLDB, contenders: list[Contender], task: str, combined_quality_scores: dict[str, float]) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    hotkey_to_effective_volumes: dict[str, float] = {}
    normalised_period_scores = {}
    period_score_multipliers = {}
    for contender in [i for i in contenders if i.task == task]:
        if contender.node_hotkey not in combined_quality_scores:
            continue
        normalised_period_score, period_score_multiplier = await _calculate_normalised_period_score(psql_db, task, contender.node_hotkey)
        effective_volume = _calculate_hotkey_effective_volume_for_task(
            combined_quality_scores[contender.node_hotkey], normalised_period_score, contender.capacity
        )
        hotkey_to_effective_volumes[contender.node_hotkey] = effective_volume
        normalised_period_scores[contender.node_hotkey] = normalised_period_score
        period_score_multipliers[contender.node_hotkey] = period_score_multiplier

    return hotkey_to_effective_volumes, normalised_period_scores, period_score_multipliers


def _normalise_volumes_for_task(effective_volumes: dict[str, float]) -> dict[str, float]:
    sum_of_effective_volumes = sum(effective_volumes.values())
    if sum_of_effective_volumes == 0:
        return {}
    return {hotkey: volume / sum_of_effective_volumes for hotkey, volume in effective_volumes.items()}


def _apply_non_linear_transformation(scores: dict[str, float]) -> dict[str, float]:
    return {hotkey: score**3 for hotkey, score in scores.items()}


async def _normalise_effective_volumes_for_task(effective_volumes: dict[str, float]) -> dict[str, float]:
    
    normalised_effective_volumes_before_non_linear = _normalise_volumes_for_task(effective_volumes)
    # logger.info(f"Normalised effective volumes before non-linear transformation: {normalised_effective_volumes_before_non_linear}\n")
    effective_volumes_after_non_linear_transformation = _apply_non_linear_transformation(normalised_effective_volumes_before_non_linear)
    normalised_scores_for_task = _normalise_volumes_for_task(effective_volumes_after_non_linear_transformation)
    return normalised_scores_for_task

async def calculate_scores_for_settings_weights(
    config_main: Config,
    contenders: list[Contender]
) -> tuple[list[int], list[float]]:
    psql_db = config_main.psql_db 
    netuid = config_main.netuid
    ss58_address = None
    while ss58_address is None:
        ss58_address = await get_vali_ss58_address(psql_db, netuid)
    
    contender_weights_info_objects: list[ContenderWeightsInfoPostObject] = []
    miner_weights_objects: list[MinerWeightsPostObject] = []

    total_hotkey_scores: dict[str, float] = {}

    task_configs = get_task_configs()
    for task, config in task_configs.items():
        if not config.enabled:
            logger.debug(f"Skipping task: {task} as it is not enabled")
            continue
        task_weight = config.weight
        logger.debug(f"Processing task: {task}, weight: {task_weight}\n")

        combined_quality_scores, average_quality_scores, metric_bonuses = await _process_quality_scores(psql_db, task, netuid)
        effective_volumes, normalised_period_scores, period_score_multipliers = await _calculate_effective_volumes_for_task(psql_db, contenders, task, combined_quality_scores)
    
        normalised_scores_for_task = await _normalise_effective_volumes_for_task(effective_volumes)

        for hotkey, score in normalised_scores_for_task.items():
            total_hotkey_scores[hotkey] = total_hotkey_scores.get(hotkey, 0) + score * task_weight
            
            contender = next((c for c in contenders if c.node_hotkey == hotkey and c.task == task), None)
            
            if contender:
                scores_info_object = ContenderWeightsInfoPostObject(
                    version_key = ccst.VERSION_KEY,
                    netuid = netuid,
                    validator_hotkey=ss58_address,
                    created_at = datetime.now(timezone.utc),
                    miner_hotkey=hotkey,
                    task=task,
                    average_quality_score=average_quality_scores.get(hotkey, 0),
                    metric_bonus=metric_bonuses.get(hotkey, 0),
                    combined_quality_score=combined_quality_scores.get(hotkey, 0),
                    period_score_multiplier=period_score_multipliers.get(hotkey, 0),
                    normalised_period_score=normalised_period_scores.get(hotkey, 0),
                    contender_capacity=contender.capacity,
                    normalised_net_score=score
                )
                contender_weights_info_objects.append(scores_info_object)
        logger.debug(f"Completed processing task: {task}")

    logger.debug("Completed calculation of scores for settings weights")

    hotkey_to_uid = {contender.node_hotkey: contender.node_id for contender in contenders}
    total_score = sum(total_hotkey_scores.values())

    node_ids, node_weights = [], []
    for hotkey, score in total_hotkey_scores.items():
        node_ids.append(hotkey_to_uid[hotkey])
        node_weights.append(score / total_score)
        miner_weight_object = MinerWeightsPostObject(
            version_key = ccst.VERSION_KEY,
            netuid = netuid,
            validator_hotkey=ss58_address,
            created_at = datetime.now(timezone.utc),
            miner_hotkey=hotkey,
            node_weight=score / total_score
        )
        miner_weights_objects.append(miner_weight_object)

    await _post_scoring_stats_to_local_db(config_main, contender_weights_info_objects, miner_weights_objects)
    await _post_scoring_stats_to_nineteen(config_main, contender_weights_info_objects, miner_weights_objects)
    
    scoring_stats_to_delete_locally = datetime.now() - timedelta(days=7)
    async with await config_main.psql_db.connection() as connection:
        await delete_weights_info_older_than(connection, scoring_stats_to_delete_locally)
        await delete_miner_weights_older_than(connection, scoring_stats_to_delete_locally)

    return node_ids, node_weights

async def _post_scoring_stats_to_local_db(config: Config, contender_weights_info_list: list[ContenderWeightsInfoPostObject], miner_weights_list: list[MinerWeightsPostObject]):
    async with await config.psql_db.connection() as conn:
        await insert_scoring_stats(
            connection=conn,
            scoring_stats=contender_weights_info_list
        )
        
        await insert_weights(
            connection=conn,
            miner_weights=miner_weights_list
        )

async def _post_scoring_stats_to_nineteen(config: Config, contender_weights_info_list: list[ContenderWeightsInfoPostObject], miner_weights_list: list[MinerWeightsPostObject]):
    await post_to_nineteen_ai(
        data_to_post=[contender_weights_info.model_dump(mode="json") for contender_weights_info in contender_weights_info_list],
        keypair=config.keypair,
        data_type_to_post=DataTypeToPost.CONTENDER_WEIGHTS_INFO,
        timeout=10
    )
    await post_to_nineteen_ai(
        data_to_post=[miner_weights.model_dump(mode="json") for miner_weights in miner_weights_list],
        keypair=config.keypair,
        data_type_to_post=DataTypeToPost.MINER_WEIGHTS,
        timeout=10
    )


###############################################################
async def calculate_scores_for_settings_weights_debug(
    psql_db: PSQLDB,
    contenders: list[Contender],
    netuid: int
) -> tuple[list[int], list[float], dict[str, dict[str, float]], dict[str, dict[str, dict[str, float]]]]:
    total_hotkey_scores: dict[str, float] = {}

    task_configs = get_task_configs()
    all_normalised_scores = {}
    detailed_scores_info = {}

    for task, config in task_configs.items():
        if not config.enabled:
            logger.debug(f"Skipping task: {task} as it is not enabled")
            continue
        task_weight = config.weight
        logger.debug(f"Processing task: {task}, weight: {task_weight}\n")

        # Calculate normalised scores and gather detailed information
        normalised_scores_for_task = await _normalise_effective_volumes_for_task(psql_db, task, contenders, netuid)
        combined_quality_scores = await _calculate_combined_quality_score(psql_db, task, netuid)
        period_scores = {
            contender.node_hotkey: await _calculate_normalised_period_score(psql_db, task, contender.node_hotkey)
            for contender in contenders
            if contender.task == task
        }
        capacities = {contender.node_hotkey: contender.capacity for contender in contenders if contender.task == task}

        # Calculate additional metrics
        reward_datas = await _get_reward_datas(psql_db, task, netuid)
        metrics = {}
        quality_scores = {}
        for reward_data in reward_datas:
            if reward_data.metric is not None and reward_data.quality_score is not None:
                metrics[reward_data.node_hotkey] = metrics.get(reward_data.node_hotkey, []) + [reward_data.metric]
                quality_scores[reward_data.node_hotkey] = quality_scores.get(reward_data.node_hotkey, []) + [
                    reward_data.quality_score
                ]

        average_weighted_quality_scores = {
            node_hotkey: sum(score**1.5 for score in scores) / len(scores) for node_hotkey, scores in quality_scores.items()
        }
        metric_scores = {node_hotkey: _get_metric_score(scores) for node_hotkey, scores in metrics.items()}
        metric_bonuses = _get_metric_bonuses(metric_scores)

        # Collect detailed information for debugging
        detailed_scores_info[task] = {
            "combined_quality_scores": combined_quality_scores,
            "period_scores": period_scores,
            "capacities": capacities,
            "normalised_scores": normalised_scores_for_task,
            "average_weighted_quality_scores": average_weighted_quality_scores,
            "metric_bonuses": metric_bonuses,
        }



        all_normalised_scores[task] = normalised_scores_for_task
        for hotkey, score in normalised_scores_for_task.items():
            total_hotkey_scores[hotkey] = total_hotkey_scores.get(hotkey, 0) + score * task_weight

        logger.debug(f"Completed processing task: {task}")

    logger.debug("Completed calculation of scores for settings weights")

    hotkey_to_uid = {contender.node_hotkey: contender.node_id for contender in contenders}
    total_score = sum(total_hotkey_scores.values())

    node_ids, node_weights = [], []
    for hotkey, score in total_hotkey_scores.items():
        node_ids.append(hotkey_to_uid[hotkey])
        node_weights.append(score / total_score)

    return node_ids, node_weights, all_normalised_scores, detailed_scores_info
