from datetime import datetime, timedelta
import os

from validator.db.database import PSQLDB

os.environ["ENV"] = "dev"

from typing import Any, Awaitable, Callable, TypeVar  # noqa
import streamlit as st  # noqa
from core import Task  # noqa
from core.logging import get_logger  # noqa
from redis.asyncio import Redis  # noqa
from validator.core.store_synthetic_data import synthetic_generation_manager  # noqa
import asyncio  # noqa
from validator.core.manage_participants import get_participant_info, scheduling_participants  # noqa
from validator.utils import participant_utils as putils, redis_utils as rutils  # noqa
from validator.utils import redis_constants as rcst  # noqa
import pandas as pd  # noqa

pd.set_option("future.no_silent_downcasting", True)

st.set_page_config(layout="wide")
st.markdown("# Vision [τ, τ] SN19 - Dev Panel")


logger = get_logger(__name__)
T = TypeVar("T")


async def _get_all_synthetic_data_versions(redis_db: Redis) -> None:
    versions = {}
    for task in Task:
        synthetic_version = await redis_db.hget(rcst.SYNTHETIC_DATA_VERSIONS_KEY, task.value)
        versions[task.value] = synthetic_version

    return versions


@st.cache_resource
def get_redis():
    return Redis(host="localhost", port=6379, db=0)


@st.cache_resource
def get_psql_db():
    psql_db =  PSQLDB()
    return psql_db


@st.cache_data
def get_synthetic_data():
    return run_in_loop(_get_all_synthetic_data_versions)


@st.cache_data(ttl=0.1)
def get_synthetic_scheduling_queue():
    return run_in_loop(putils.load_synthetic_scheduling_queue)


@st.cache_data
def get_participants():
    participants = run_in_loop(putils.load_participants, with_psql=True, with_redis=False)
    participants = [{**participant.model_dump(), **{"id": participant.id}} for participant in participants]
    for participant in participants:
        participant["task"] = participant["task"].value
    return participants


@st.cache_data(ttl=0.1)
def get_query_queue():
    participants = run_in_loop(putils.load_query_queue)

    return participants


@st.cache_resource(show_spinner=True)
def get_event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def run_in_loop(
    func: Callable[[Any, Any], Awaitable[T]],
    *args: Any,
    with_redis: bool = True,
    with_psql: bool = False,
    create_task: bool = False,
    **kwargs: Any,
) -> T:
    loop = get_event_loop()

    if create_task:
        if with_redis:
            return loop.create_task(run_with_redis(func, *args, **kwargs))
        elif with_psql:
            return loop.create_task(run_with_psql(func, *args, **kwargs))
        else:
            return loop.create_task(func(*args, **kwargs))
    else:
        if with_redis:
            return loop.run_until_complete(run_with_redis(func, *args, **kwargs))
        elif with_psql:
            return loop.run_until_complete(run_with_psql(func, *args, **kwargs))
        else:
            return loop.run_until_complete(func(*args, **kwargs))


async def run_with_redis(func: Callable[[Redis, Any, Any], Awaitable[T]], *args: Any, **kwargs: Any) -> T:
    redis = get_redis()
    try:
        return await func(redis, *args, **kwargs)
    finally:
        await redis.aclose()


async def run_with_psql(func: Callable[[Redis, Any, Any], Awaitable[T]], *args: Any, **kwargs: Any) -> T:
    psql_db = get_psql_db()
    await psql_db.connect()
    try:
        return await func(psql_db, *args, **kwargs)
    finally:
        ...


async def clear_redis(redis_db: Redis):
    await redis_db.flushall()
    st.cache_data.clear()

async def clear_psql(psql_db: PSQLDB):
    await psql_db.truncate_all_tables()
    st.cache_data.clear()

if "participants_being_scheduled" not in st.session_state:
    st.session_state["participants_being_scheduled"] = {}

####################################################################
################# State #######################################


####################################################################
################# Top level buttons #######################################
with st.container():
    st.markdown("---")  # Horizontal line above the box
    top_row_col1, top_row_col2 = st.columns(2)

    with top_row_col1:
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Clear Redis"):
                run_in_loop(clear_redis)

        with col2:
            if st.button("Clear PSQL"):
                run_in_loop(clear_psql, with_psql=True, with_redis=False)

    st.markdown("---")

####################################################################
################# First row #######################################

col1, col2 = st.columns(2)


##########################
### Synthetic Data #######

with col1:
    st.subheader("Synthetic Data Management")

    model_options = [t.value for t in Task]
    selected_model = st.selectbox("Select Model", model_options, index=0)

    if st.button("Add Synthetic Data"):
        run_in_loop(synthetic_generation_manager.patched_update_synthetic_data, task=Task(selected_model))
        st.cache_data.clear()

    synthetic_data = get_synthetic_data()
    if synthetic_data:
        st.write(synthetic_data)

##########################
### Participants #######

with col2:
    st.subheader("Participants Management")

    number_of_participants = st.number_input("Number of participants", min_value=1, value=1)
    if st.button("Add Fake Participants"):
        run_in_loop(get_participant_info.main, with_redis=False)

        st.cache_data.clear()

    participants = get_participants()
    edited_participants = st.data_editor(participants, num_rows="dynamic", key="participant_editor")
    if st.button("Save participants"):
        run_in_loop(rutils.delete_key_from_redis, rcst.PARTICIPANT_IDS_KEY)
        for participant in edited_participants:
            run_in_loop(
                get_participant_info.store_participants,
                task=participant["task"],
                hotkey=participant["hotkey"],
                declared_volume=participant["declared_volume"],
                volume_to_score=participant["volume_to_score"],
                synthetic_requests_still_to_make=participant["synthetic_requests_still_to_make"],
                delay_between_synthetic_requests=participant["delay_between_synthetic_requests"],
            )
        st.cache_data.clear()


####################################################################
################# Second row #######################################

##########################
### Scheduling #######
st.markdown("---")
with st.container():
    st.markdown("<h1 style='text-align: center;'>Queues</h1>", unsafe_allow_html=True)

    st.markdown("- - -")
    tc1, tc2 = st.columns(2)

    with tc1:
        participant_id = st.selectbox("Select participant", [participant["id"] for participant in get_participants()])

    with tc2:
        schedule_in_x_seconds = st.number_input("Schedule synthetic query in X seconds", min_value=1, value=1)

    st.markdown("- - -")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Synthetic Scheduling Queue")

        if st.button("Schedule Synthetic Queries for participant"):
            time_to_schedule_for = datetime.now() + timedelta(seconds=schedule_in_x_seconds)
            run_in_loop(
                scheduling_participants.schedule_synthetic_query,
                participant_id,
                timestamp=time_to_schedule_for.timestamp(),
            )
            st.cache_data.clear()

        scheduled_synthetic_queries = get_synthetic_scheduling_queue()

        if scheduled_synthetic_queries:
            st.write(scheduled_synthetic_queries)
    with col2:
        st.subheader("Query queue")

        if st.button("Add synthetic queries which are ready"):
            st.cache_data.clear()
            run_in_loop(scheduling_participants.run_schedule_processor, with_redis=True, run_once=True)
            st.cache_data.clear()

        synthetic_query_list = get_query_queue()
        if synthetic_query_list:
            st.write(synthetic_query_list)


st.markdown("---")
##########################
### Participants #######

col4, col5 = st.columns(2)
with col4:
    st.header("Ongoing")
    st.subheader("Scheduling")

log_display = st.empty()


# col5, col6 = st.columns(2)

# with col5:
#     st.subheader("Organic Queries")

# with col6:
#     st.subheader("Weight setting")
