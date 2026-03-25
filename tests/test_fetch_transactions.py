import json
import pytest
from unittest.mock import patch, MagicMock
from fetch_transactions import TransactionFetcher


@pytest.fixture
def fetcher(tmp_path):
    return TransactionFetcher(
        endpoint="https://graphql.example.com/v1/graphql",
        address="cheqd1testwalletaddress",
        batch_size=10,
        max_transactions=100,
        output_file=str(tmp_path / "txs.json"),
    )


def make_batch(count, start_height=1000):
    """Build a fake GraphQL response batch."""
    return [
        {
            "transaction": {
                "height": start_height + i,
                "hash": f"HASH{start_height + i}",
                "success": True,
                "messages": [],
                "logs": [],
                "fee": {},
                "block": {
                    "height": start_height + i,
                    "timestamp": "2024-01-15T10:30:00Z",
                },
            }
        }
        for i in range(count)
    ]


class TestFetcherInit:
    def test_default_output_file(self):
        f = TransactionFetcher("http://x", "addr1")
        assert "transactions_" in str(f.output_file)
        assert str(f.output_file).endswith(".json")

    def test_custom_output_file(self, tmp_path):
        f = TransactionFetcher("http://x", "addr1", output_file=str(tmp_path / "custom.json"))
        assert str(f.output_file).endswith("custom.json")


class TestProgress:
    def test_no_progress_file(self, fetcher):
        progress = fetcher.load_progress()
        assert progress["offset"] == 0
        assert progress["transactions"] == []

    def test_save_and_load(self, fetcher):
        fetcher.save_progress(50, [{"tx": 1}, {"tx": 2}])
        progress = fetcher.load_progress()
        assert progress["offset"] == 50
        assert len(progress["transactions"]) == 2


class TestFetchBatch:
    @patch("fetch_transactions.requests.post")
    def test_successful_batch(self, mock_post, fetcher):
        batch = make_batch(5)
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"messagesByAddress": batch}},
        )
        mock_post.return_value.raise_for_status = MagicMock()
        result = fetcher.fetch_batch(0)
        assert len(result) == 5

    @patch("fetch_transactions.requests.post")
    def test_graphql_error(self, mock_post, fetcher):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"errors": [{"message": "bad query"}]},
        )
        mock_post.return_value.raise_for_status = MagicMock()
        with pytest.raises(Exception, match="GraphQL errors"):
            fetcher.fetch_batch(0, retries=1)

    @patch("fetch_transactions.requests.post")
    @patch("fetch_transactions.time.sleep")
    def test_retry_on_failure(self, mock_sleep, mock_post, fetcher):
        batch = make_batch(3)
        mock_post.side_effect = [
            Exception("timeout"),
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"messagesByAddress": batch}},
                raise_for_status=MagicMock(),
            ),
        ]
        result = fetcher.fetch_batch(0, retries=2)
        assert len(result) == 3
        mock_sleep.assert_called_once_with(2)


class TestFetchAll:
    @patch("fetch_transactions.time.sleep")
    @patch("fetch_transactions.requests.post")
    def test_fetches_until_empty(self, mock_post, mock_sleep, fetcher):
        # Ensure no leftover progress file
        if fetcher.progress_file.exists():
            fetcher.progress_file.unlink()

        batch1 = make_batch(10, start_height=1000)
        batch2 = make_batch(5, start_height=1010)
        mock_post.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda b=b: {"data": {"messagesByAddress": b}},
                raise_for_status=MagicMock(),
            )
            for b in [batch1, batch2, []]
        ]
        result = fetcher.fetch_all()
        assert result is True
        assert fetcher.output_file.exists()
        with open(fetcher.output_file) as f:
            data = json.load(f)
        assert len(data) == 15

    @patch("fetch_transactions.time.sleep")
    @patch("fetch_transactions.requests.post")
    def test_respects_max_transactions(self, mock_post, mock_sleep, tmp_path):
        fetcher = TransactionFetcher(
            "http://x", "addr1", batch_size=10, max_transactions=15,
            output_file=str(tmp_path / "txs.json"),
        )
        batch1 = make_batch(10, start_height=1000)
        batch2 = make_batch(10, start_height=1010)
        mock_post.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda b=b: {"data": {"messagesByAddress": b}},
                raise_for_status=MagicMock(),
            )
            for b in [batch1, batch2]
        ]
        fetcher.fetch_all()
        with open(fetcher.output_file) as f:
            data = json.load(f)
        assert len(data) == 15

    @patch("fetch_transactions.time.sleep")
    @patch("fetch_transactions.requests.post")
    def test_saves_progress_on_error(self, mock_post, mock_sleep, fetcher):
        batch1 = make_batch(10, start_height=1000)
        mock_post.side_effect = [
            MagicMock(
                status_code=200,
                json=lambda: {"data": {"messagesByAddress": batch1}},
                raise_for_status=MagicMock(),
            ),
            Exception("connection lost"),
        ]
        result = fetcher.fetch_all()
        assert result is False
        assert fetcher.progress_file.exists()

    @patch("fetch_transactions.time.sleep")
    @patch("fetch_transactions.requests.post")
    def test_cleans_up_progress_on_success(self, mock_post, mock_sleep, fetcher):
        # Create a progress file first
        fetcher.save_progress(0, [])
        assert fetcher.progress_file.exists()

        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"messagesByAddress": []}},
            raise_for_status=MagicMock(),
        )
        fetcher.fetch_all()
        assert not fetcher.progress_file.exists()
