from sqlalchemy import Column, Integer, String, ForeignKey, Table, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB


Base = declarative_base()


class OrderbookSnapshot(Base):
    __tablename__ = "orderbook_snapshots"
    id = Column(Integer, primary_key=True, comment="autoincrementing id")
    created_at = Column(
        DateTime, default=datetime.now, comment="created_at date. datetime in DB"
    )
    received_at = Column(
        DateTime,
        comment="received_at datetime.",
    )
    external_time = Column(
        DateTime,
        comment="datetime from provider. can be null if datetime is not provided by provider",
    )
    base = Column(String, comment="internal representation of base asset")
    counter = Column(String, comment="internal representation of counter asset")
    provider = Column(String, comment="reference to the provider of the OrderbookSnapshot")
    levels = Column(Integer, comment="number of levels in the snapshot. if -1 means all levels")
    bids = Column(
        String,
        comment="bids in a 2 levels json array [['price','size'],['price','size']]. 'price' and 'size' are kept as strings to avoid numerical problems",
    )
    asks = Column(
        String,
        comment="bids in a 2 levels json array [['price','size'],['price','size']]. 'price' and 'size' are kept as strings to avoid numerical problems",
    )

    def __init__(self, external_time, received_at, base, counter, provider, levels, bids, asks):
        self.external_time = external_time
        self.received_at = received_at
        self.base = base
        self.counter = counter
        self.provider = provider
        self.levels = levels
        self.bids = bids
        self.asks = asks


class TickerInformation(Base):
    __tablename__ = "ticker_information"
    id = Column(Integer, primary_key=True, comment="autoincrementing id")
    created_at = Column(
        DateTime, default=datetime.now, comment="created_at date. datetime in DB"
    )
    ticker_id = Column(String, comment="ticker ID - used for requests that require it")
    base = Column(String, comment="internal representation of base asset")
    counter = Column(String, comment="internal representation of counter asset")
    provider = Column(String, comment="reference to the provider of the OrderbookSnapshot")
    last_price = Column(Float, comment="last price for the ticker")
    base_volume = Column(Float, comment="base volume")
    counter_volume = Column(Float, comment="quote volume")
    market_type = Column(String, comment="type of market SPOT/PERP")

    def __init__(self, ticker_id, base, counter, provider, last_price, base_volume, counter_volume, market_type):
        self.ticker_id = ticker_id
        self.base = base
        self.counter = counter
        self.provider = provider
        self.last_price = last_price
        self.base_volume = base_volume
        self.counter_volume = counter_volume
        self.market_type  = market_type


class OrderbookLevelOverride(Base):
    __tablename__ = "orderbook_level_overrides"
    id = Column(Integer, primary_key=True, comment="autoincrementing id")
    orderbook_snapshot_id = Column(
        Integer,
        comment="id of orderbook_snapshot to which this OrderbookLevelOverride relates to",
    )
    created_at = Column(
        DateTime, default=datetime.now, comment="created_at date. datetime in DB"
    )
    received_at = Column(
        DateTime,
        comment="received_at datetime.",
    )
    external_time = Column(
        DateTime,
        comment="datetime from provider. can be null if datetime is not provided by provider",
    )
    base = Column(String, comment="internal representation of base asset")
    counter = Column(String, comment="internal representation of counter asset")
    provider = Column(String, comment="reference to the provider of the OrderbookLevelOverride")
    bids_overrides = Column(
        String,
        comment="bids overrides in a 2 levels json array [['price','size'],['price','size']]. 'price' and 'size' are kept as strings to avoid numerical problems. the existing level for that price needs to be replaced with this new information. size =0 means that that level can be removed from book.",
    )
    asks_overrides = Column(
        String,
        comment="asks overrides in a 2 levels json array [['price','size'],['price','size']]. 'price' and 'size' are kept as strings to avoid numerical problems. the existing level for that price needs to be replaced with this new information. size =0 means that that level can be removed from book.",
    )

    def __init__(
        self,
        orderbook_snapshot_id,
        external_time,
        received_at,
        base,
        counter,
        provider,
        bids_overrides,
        asks_overrides,
    ):
        self.orderbook_snapshot_id = orderbook_snapshot_id
        self.external_time = external_time
        self.received_at = received_at
        self.base = base
        self.counter = counter
        self.provider = provider
        self.bids_overrides = bids_overrides
        self.asks_overrides = asks_overrides

class OrderbookLevelDiff(Base):
    __tablename__ = "orderbook_level_diffs"
    id = Column(Integer, primary_key=True, comment="autoincrementing id")
    orderbook_snapshot_id = Column(
        Integer,
        comment="id of orderbook_snapshot to which this OrderbookLevelDiff relates to",
    )
    created_at = Column(
        DateTime, default=datetime.now, comment="created_at date. datetime in DB"
    )
    received_at = Column(
        DateTime,
        comment="received_at datetime.",
    )
    external_time = Column(
        DateTime,
        comment="datetime from provider. can be null if datetime is not provided by provider",
    )
    base = Column(String, comment="internal representation of base asset")
    counter = Column(String, comment="internal representation of counter asset")
    provider = Column(String, comment="reference to the provider of the OrderbookLevelDiff")
    bids_changes = Column(
        String,
        comment="bids changes in a 2 levels json array [['price','size','type'],['price','size','action_type']]. 'price' and 'size' are kept as strings to avoid numerical problems. 'action_type' can be 'ADD' or 'REMOVE'.the quantity needs to be added to the book if 'ADD' and removed from the book if 'REMOVE'",
    )
    asks_changes = Column(
        String,
        comment="asks changes in a 2 levels json array [['price','size','type'],['price','size','action_type']]. 'price' and 'size' are kept as strings to avoid numerical problems. 'action_type' can be 'ADD' or 'REMOVE'.the quantity needs to be added to the book if 'ADD' and removed from the book if 'REMOVE'",
    )

    def __init__(
        self,
        orderbook_snapshot_id,
        external_time,
        received_at,
        base,
        counter,
        provider,
        bids_changes,
        asks_changes,
    ):
        self.orderbook_snapshot_id = orderbook_snapshot_id
        self.external_time = external_time
        self.received_at = received_at
        self.base = base
        self.counter = counter
        self.provider = provider
        self.bids_changes = bids_changes
        self.asks_changes = asks_changes
