from datetime import date

from tiingo_data_pull.clients.notion_client import NotionClient, NotionPropertyConfig
from tiingo_data_pull.models.price_bar import PriceBar


class DummySession:
    def post(self, *args, **kwargs):  # pragma: no cover - HTTP not expected in unit tests
        raise AssertionError("HTTP requests should not be made in this test")


def sample_price() -> PriceBar:
    return PriceBar(
        ticker="AAPL",
        date=date(2024, 1, 2),
        open=100.0,
        close=110.0,
        high=115.0,
        low=95.0,
        volume=1_000,
    )


def test_price_payload_sets_title_property_when_ticker_is_rich_text() -> None:
    config = NotionPropertyConfig(
        ticker_property="Ticker",
        ticker_property_type="rich_text",
        page_title_property="Security Name",
    )
    client = NotionClient("key", "db", property_config=config, session=DummySession())

    payload = client._price_to_page_payload(sample_price())

    ticker_rich_text = payload["properties"]["Ticker"]
    assert ticker_rich_text == {
        "rich_text": [{"type": "text", "text": {"content": "AAPL"}}]
    }
    assert payload["properties"]["Security Name"] == {
        "title": [{"type": "text", "text": {"content": "AAPL"}}]
    }


def test_price_payload_uses_ticker_property_when_type_is_title() -> None:
    config = NotionPropertyConfig(
        ticker_property="Ticker",
        ticker_property_type="title",
        page_title_property="Security Name",
    )
    client = NotionClient("key", "db", property_config=config, session=DummySession())

    payload = client._price_to_page_payload(sample_price())

    assert payload["properties"]["Ticker"] == {
        "title": [{"type": "text", "text": {"content": "AAPL"}}]
    }
    assert "Security Name" not in payload["properties"]
