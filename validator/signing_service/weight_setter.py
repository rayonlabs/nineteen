from substrateinterface import SubstrateInterface, Keypair
from validator.signing_service import utils

logger = utils.get_logger(__name__)


def set_weights(
    substrate: SubstrateInterface,
    keypair: Keypair,
    uids: list[int],
    vals: list[float],
    netuid: int,
    version_key: int,
    wait_for_inclusion=True,
    wait_for_finalization=True,
) -> bool:
    def _set_weights():
        call = substrate.compose_call(
            call_module="SubtensorModule",
            call_function="set_weights",
            call_params={
                "dests": uids,
                "weights": vals,
                "netuid": netuid,
                "version_key": version_key,
            },
        )
        # Period dictates how long the extrinsic will stay as part of waiting pool
        extrinsic = substrate.create_signed_extrinsic(
            call=call,
            keypair=keypair,
            era={"period": 5},
        )
        response = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=wait_for_inclusion,
            wait_for_finalization=wait_for_finalization,
        )

        if not wait_for_finalization and not wait_for_inclusion:
            return True, "Not waiting for finalization or inclusion."

        response.process_events()
        if response.is_success:
            return True, "Successfully set weights."
        else:
            return False, utils.format_weights_error_message(response.error_message)

    return utils.retry(_set_weights, logger, tries=20, delay=1, max_delay=60, backoff=2, jitter=1)
