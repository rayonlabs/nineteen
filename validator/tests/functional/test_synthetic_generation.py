import pytest
from unittest.mock import patch, AsyncMock
from validator.query_node.src.process_queries import process_task
from validator.utils.redis import redis_dataclasses as rdc
from core import task_config as tcfg

@pytest.mark.asyncio
class TestSyntheticGeneration:
    @patch('validator.query_node.src.process_queries._acknowledge_job')
    @patch('validator.query_node.src.process_queries._handle_error')
    @patch('validator.query_node.src.process_queries._handle_stream_query')
    async def test_process_task_synthetic_chat_generation(self, mock_handle_stream_query, mock_handle_error, mock_acknowledge_job):
        config = AsyncMock()
        config.redis_db = AsyncMock()

        message = rdc.QueryQueueMessage(
            query_type="synthetic",
            job_id="test_job_id",
            task="chat_llama_3",
            query_payload=None
        )

        task_configs = tcfg.task_configs_factory()
        task_config = task_configs[tcfg.CHAT_LLAMA_3_2_3B]

        with patch('validator.query_node.src.process_queries.tcfg.get_enabled_task_config', return_value=task_config):
            await process_task(config, message)

            mock_acknowledge_job.assert_not_called()
            mock_handle_error.assert_not_called()
            
            mock_handle_stream_query.assert_called_once()
            assert isinstance(mock_handle_stream_query.call_args.kwargs['message'].query_payload, dict)
            assert "messages" in mock_handle_stream_query.call_args.kwargs['message'].query_payload

    @patch('validator.query_node.src.process_queries._acknowledge_job')
    @patch('validator.query_node.src.process_queries._handle_error')
    @patch('validator.query_node.src.process_queries._handle_nonstream_query')
    async def test_process_task_synthetic_text_to_image_generation(self, mock_handle_nonstream_query, mock_handle_error, mock_acknowledge_job):
        config = AsyncMock()
        config.redis_db = AsyncMock()

        message = rdc.QueryQueueMessage(
            query_type="synthetic",
            job_id="test_job_id",
            task="text_to_image",
            query_payload=None
        )

        task_configs = tcfg.task_configs_factory()
        task_config = task_configs[tcfg.PROTEUS_TEXT_TO_IMAGE]

        with patch('validator.query_node.src.process_queries.tcfg.get_enabled_task_config', return_value=task_config):
            await process_task(config, message)

            mock_acknowledge_job.assert_not_called()
            mock_handle_error.assert_not_called()

            mock_handle_nonstream_query.assert_called_once()
            
            assert isinstance(mock_handle_nonstream_query.call_args.kwargs['message'].query_payload, dict)
            assert mock_handle_nonstream_query.call_args.kwargs['message'].query_payload["prompt"] is not None

    @patch('validator.query_node.src.process_queries._acknowledge_job')
    @patch('validator.query_node.src.process_queries._handle_error')
    @patch('validator.query_node.src.process_queries._handle_nonstream_query')
    async def test_process_task_synthetic_image_to_image_generation(self, mock_handle_nonstream_query, mock_handle_error, mock_acknowledge_job):
        config = AsyncMock()
        config.redis_db = AsyncMock()
        
        # Mock the redis calls
        config.redis_db.scard.return_value = 1
        config.redis_db.srandmember.return_value = "mocked_image_key"
        config.redis_db.get.return_value = "mocked_base64_image"

        message = rdc.QueryQueueMessage(
            query_type="synthetic",
            job_id="test_job_id",
            task="image_to_image",
            query_payload=None
        )

        task_configs = tcfg.task_configs_factory()
        task_config = task_configs[tcfg.PROTEUS_IMAGE_TO_IMAGE]

        with patch('validator.query_node.src.process_queries.tcfg.get_enabled_task_config', return_value=task_config):
            await process_task(config, message)

            mock_acknowledge_job.assert_not_called()
            mock_handle_error.assert_not_called()
            
            mock_handle_nonstream_query.assert_called_once()
            
            assert isinstance(mock_handle_nonstream_query.call_args.kwargs['message'].query_payload, dict)
            assert mock_handle_nonstream_query.call_args.kwargs['message'].query_payload["init_image"] is not None

    