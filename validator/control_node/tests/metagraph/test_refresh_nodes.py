import unittest
from unittest.mock import AsyncMock, patch
from asyncpg import Connection

from validator.db.src.database import PSQLDB
from validator.control_node.src.cycle.refresh_nodes import Config, get_and_store_nodes
from fiber.networking.models import NodeWithFernet as Node


class TestRefreshNodes(unittest.IsolatedAsyncioTestCase):
    def create_test_config(self) -> Config:
        return Config(
            psql_db=PSQLDB(),
            run_once=True,
            test_env=True,
            subtensor_network="test",
            netuid=1,
            substrate=AsyncMock(),
            keypair=AsyncMock(),
        )

    async def asyncSetUp(self):
        self.config = self.create_test_config()
        await self.config.psql_db.connect()

    async def asyncTearDown(self):
        await self.config.psql_db.close()

    @patch("validator.chain_node.src.refresh_nodes.fetch_nodes_from_metagraph")
    async def test_get_and_store_nodes(self, mock_fetch_nodes):
        # Mock the fetch_nodes_from_metagraph function
        mock_fetch_nodes.return_value = [
            Node(
                hotkey="test-hotkey1",
                coldkey="test-coldkey1",
                node_id=1,
                incentive=0.1,
                netuid=1,
                stake=30.0,
                trust=0.8,
                vtrust=0.7,
                ip="127.0.0.1",
                ip_type=4,
                port=1,
                protocol=4,
            ),
            Node(
                hotkey="test-hotkey2",
                coldkey="test-coldkey2",
                node_id=2,
                incentive=0.2,
                netuid=1,
                stake=20.0,
                trust=0.9,
                vtrust=0.8,
                ip="127.0.0.1",
                ip_type=4,
                port=2,
                protocol=4,
            ),
            Node(
                hotkey="test-vali",
                coldkey="test-vali-ck",
                node_id=3,
                incentive=0.3,
                netuid=1,
                stake=50.0,
                trust=1.0,
                vtrust=0.9,
                ip="127.0.0.1",
                ip_type=4,
                port=3,
                protocol=4,
            ),
        ]

        async with await self.config.psql_db.connection() as conn:
            conn: Connection

            await conn.execute("DELETE FROM nodes; DELETE FROM nodes_history;")

            await get_and_store_nodes(self.config)

            nodes = await conn.fetch("SELECT * FROM nodes ORDER BY hotkey")
            self.assertEqual(len(nodes), 3, "Expected 3 nodes in node table")

            self.assertEqual(nodes[0]["hotkey"], "test-hotkey1")
            self.assertEqual(nodes[1]["hotkey"], "test-hotkey2")
            self.assertEqual(nodes[2]["hotkey"], "test-vali")

            self.assertEqual(nodes[0]["node_id"], 1)
            self.assertEqual(nodes[1]["incentive"], 0.2)
            self.assertEqual(nodes[2]["stake"], 50.0)

            await get_and_store_nodes(self.config)

            node_history = await conn.fetch("SELECT * FROM nodes_history ORDER BY hotkey")
            self.assertEqual(len(node_history), 3, "Expected 3 entries in node_history table")


if __name__ == "__main__":
    unittest.main()
