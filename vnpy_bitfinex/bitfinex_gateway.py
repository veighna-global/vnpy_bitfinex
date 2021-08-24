import hashlib
import hmac
import sys
import time
from copy import copy
from datetime import datetime, timedelta
from urllib.parse import urlencode
from typing import Dict, Any, List, Tuple
import pytz

from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event, EventEngine

from vnpy.trader.constant import (
    Direction,
    Exchange,
    OrderType,
    Product,
    Status,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    BarData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)

# UTC时区
UTC_TZ = pytz.utc

# BASE_URL = "https://api.bitfinex.com/"

# 实盘REST API地址
REST_HOST: str = "https://api.bitfinex.com/"

# 实盘Websocket API地址
WEBSOCKET_HOST: str = "wss://api-pub.bitfinex.com/ws/2"

# 委托状态映射
STATUS_BITFINEX2VT: Dict[str, Status] = {
    "ACTIVE": Status.NOTTRADED,
    "PARTIALLY FILLED": Status.PARTTRADED,
    "EXECUTED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
}

# 委托类型映射
ORDERTYPE_VT2BITFINEX: Dict[OrderType, str] = {
    OrderType.LIMIT: "EXCHANGE LIMIT",
    OrderType.MARKET: "EXCHANGE MARKET",
}
ORDERTYPE_BITFINEX2VT = {
    "EXCHANGE LIMIT": OrderType.LIMIT,
    "EXCHANGE MARKET": OrderType.MARKET,
    "LIMIT": OrderType.LIMIT,
    "MARKET": OrderType.MARKET
}

# 买卖方向映射
DIRECTION_VT2BITFINEX: Dict[Direction, str] = {
    Direction.LONG: "Buy",
    Direction.SHORT: "Sell",
}
DIRECTION_BITFINEX2VT = {
    "Buy": Direction.LONG,
    "Sell": Direction.SHORT,
}

# 数据频率映射
INTERVAL_VT2BITFINEX: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1D",
}

# 时间间隔映射
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}


class BitfinexGateway(BaseGateway):
    """
    vn.py用于对接Bitfinex交易所的交易接口。
    """

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "代理地址": "",
        "代理端口": 0,
        "margin": ["False", "True"]
    }

    exchanges: Exchange = [Exchange.BITFINEX]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "BITFINEX") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.timer_count: int = 0
        self.resubscribe_interval: int = 60

        self.rest_api: "BitfinexRestApi" = BitfinexRestApi(self)
        self.ws_api: "BitfinexWebsocketApi" = BitfinexWebsocketApi(self)

    def connect(self, setting: dict):
        """连接交易接口"""
        key: str = setting["key"]
        secret: str = setting["secret"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]

        if setting["margin"] == "True":
            margin = True
        else:
            margin = False

        self.rest_api.connect(key, secret, proxy_host, proxy_port)
        self.ws_api.connect(key, secret, proxy_host, proxy_port, margin)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.ws_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.ws_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.timer_count += 1

        if self.timer_count < self.resubscribe_interval:
            return

        self.timer_count: int = 0
        self.ws_api.resubscribe()


class BitfinexRestApi(RestClient):
    """Bitfinex的REST接口"""

    def __init__(self, gateway: BitfinexGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BitfinexGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.order_count: int = 1000000
        self.connect_time: int = 0

    def sign(self, request: Request) -> Request:
        """生成Bitfinex签名"""
        nonce: str = str(int(round(time.time() * 1000000)))

        if request.params:
            query: str = urlencode(request.params)
            path: str = request.path + "?" + query
        else:
            path: str = request.path

        if request.data:
            request.data = urlencode(request.data)
        else:
            request.data = ""

        msg: str = request.method + \
            "/api/v2/{}{}{}".format(path, nonce, request.data)
        signature: str = hmac.new(
            self.secret, msg.encode("utf8"), digestmod=hashlib.sha384
        ).hexdigest()

        # 添加请求头
        headers: dict = {
            "bfx-nonce": nonce,
            "bfx-apikey": self.key,
            "bfx-signature": signature,
            "content-type": "application/json"
        }

        request.headers = headers
        return request

    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret.encode()

        self.connect_time = (
            int(datetime.now(UTC_TZ).strftime("%y%m%d%H%M%S")) * self.order_count
        )

        self.init(REST_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("REST API启动成功")
        self.query_contract()

    def query_contract(self) -> None:
        """查询合约信息"""
        self.add_request(
            method="GET",
            path="/v1/symbols_details",
            callback=self.on_query_contract,
        )

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        history: List[BarData] = []
        limit: int = 5000

        interval: str = INTERVAL_VT2BITFINEX[req.interval]
        path: str = f"/v2/candles/trade:{interval}:t{req.symbol}/hist"

        start_time: datetime = req.start

        while True:
            # 创建查询参数
            params: dict = {
                "limit": 5000,
                "start": datetime.timestamp(start_time) * 1000,
                "sort": 1
            }

            resp: Response = self.request(
                "GET",
                path,
                params=params
            )

            if resp.status_code // 100 != 2:
                msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"获取历史数据为空，开始时间：{start_time}"
                    break

                buf: List[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=row[5],
                        open_price=row[1],
                        high_price=row[3],
                        low_price=row[4],
                        close_price=row[2],
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime
                msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # 如果收到了最后一批数据则终止循环
                if len(data) < limit:
                    break

                # 更新开始时间
                start_time: datetime = bar.datetime + TIMEDELTA_MAP[req.interval]

        return history

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        for d in data:
            contract: ContractData = ContractData(
                symbol=d["pair"].upper(),
                exchange=Exchange.BITFINEX,
                name=d["pair"].upper(),
                product=Product.SPOT,
                size=1,
                pricetick=1 / pow(10, d["price_precision"]),
                min_volume=float(d["minimum_order_size"]),
                history_data=True,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)

        self.gateway.write_log("账户资金查询成功")

    def on_failed(self, status_code: int, request: Request) -> None:
        """请求失败的回调"""
        msg: str = f"请求失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """请求触发异常的回调"""
        msg: str = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )


class BitfinexWebsocketApi(WebsocketClient):
    """Bitfinex的Websocket接口"""

    def __init__(self, gateway: BitfinexGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BitfinexGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.order_id: int = 1000000
        self.trade_id: int = 1000000
        self.key: str = ""
        self.secret: str = ""

        self.ticks: Dict[str, TickData] = {}
        self.accounts: Dict[str, AccountData] = {}

        self.bids: Dict[str, Dict[float, float]] = {}
        self.asks: Dict[str, Dict[float, float]] = {}
        self.channels: Dict[str, Tuple[str, str]] = {}

        self.subscribed: Dict[str, SubscribeRequest] = {}

    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int,
        margin: bool
    ) -> None:
        """连接Websocket"""
        self.key = key
        self.secret = secret.encode()
        self.margin = margin
        self.init(WEBSOCKET_HOST, proxy_host, proxy_port)
        self.start()

    def subscribe(self, req: SubscribeRequest) -> int:
        """订阅行情"""
        if req.symbol not in self.subscribed:
            # 缓存订阅记录
            self.subscribed[req.symbol] = req

        d: dict = {
            "event": "subscribe",
            "channel": "book",
            "symbol": req.symbol,
        }
        self.send_packet(d)

        d: dict = {
            "event": "subscribe",
            "channel": "ticker",
            "symbol": req.symbol,
        }
        self.send_packet(d)

        return int(round(time.time() * 1000))

    def resubscribe(self) -> int:
        """重新订阅"""
        for req in self.subscribed.values():
            self.subscribe(req)

    def _gen_unqiue_cid(self) -> int:
        """获取唯一CID"""
        self.order_id += 1
        local_oid: str = time.strftime("%y%m%d") + str(self.order_id)
        return int(local_oid)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        orderid: int = self._gen_unqiue_cid()

        if req.direction == Direction.LONG:
            amount: float = req.volume
        else:
            amount: float = -req.volume

        order_type: str = ORDERTYPE_VT2BITFINEX[req.type]
        if self.margin:
            order_type: str = order_type.replace("EXCHANGE ", "")

        o: dict = {
            "cid": orderid,
            "type": order_type,
            "symbol": "t" + req.symbol,
            "amount": str(amount),
            "price": str(req.price),
        }

        request: list = [0, "on", None, o]

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.send_packet(request)

        self.gateway.on_order(order)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        orderid: str = req.orderid
        date_str: str = "20" + str(orderid)[0:6]
        date: str = date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:8]

        request: list = [
            0,
            "oc",
            None,
            {
                "cid": int(orderid),
                "cid_date": date
            }
        ]

        self.send_packet(request)

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("Websocket API连接成功")
        self.authenticate()

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("Websocket API连接断开")

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if isinstance(packet, dict):
            self.on_response(packet)
        else:
            self.on_update(packet)

    def on_response(self, data) -> None:
        """订阅频道回报"""
        if "event" not in data:
            return

        if data["event"] == "subscribed":
            symbol: str = str(data["symbol"].replace("t", ""))
            self.channels[data["chanId"]] = (data["channel"], symbol)

    def on_update(self, data) -> None:
        """订阅频道更新"""
        if data[1] == "hb":
            return

        channel_id: str = data[0]

        if not channel_id:
            self.on_trade_update(data)
        else:
            self.on_data_update(data)

    def on_data_update(self, data):
        """"""
        channel_id: str = data[0]
        channel, symbol = self.channels[channel_id]
        symbol: str = str(symbol.replace("t", ""))

        # 获取TICK对象
        if symbol in self.ticks:
            tick: TickData = self.ticks[symbol]
        else:
            tick: TickData = TickData(
                symbol=symbol,
                exchange=Exchange.BITFINEX,
                name=symbol,
                datetime=datetime.now(UTC_TZ),
                gateway_name=self.gateway_name,
            )

            self.ticks[symbol] = tick

        l_data1: list = data[1]

        # 更新行情
        if channel == "ticker":
            tick.volume = float(l_data1[-3])
            tick.high_price = float(l_data1[-2])
            tick.low_price = float(l_data1[-1])
            tick.last_price = float(l_data1[-4])
            tick.open_price = float(tick.last_price - l_data1[4])

        # 更新深度行情
        elif channel == "book":
            bid: dict = self.bids.setdefault(symbol, {})
            ask: dict = self.asks.setdefault(symbol, {})

            if len(l_data1) > 3:
                for price, count, amount in l_data1:
                    price: float = float(price)
                    count: int = int(count)
                    amount: float = float(amount)

                    if amount > 0:
                        bid[price] = amount
                    else:
                        ask[price] = -amount
            else:
                price, count, amount = l_data1
                price: float = float(price)
                count: int = int(count)
                amount: float = float(amount)

                if not count:
                    if price in bid:
                        del bid[price]
                    elif price in ask:
                        del ask[price]
                else:
                    if amount > 0:
                        bid[price] = amount
                    else:
                        ask[price] = -amount

            try:
                bid_keys: float = bid.keys()
                bidPriceList: list = sorted(bid_keys, reverse=True)

                tick.bid_price_1 = bidPriceList[0]
                tick.bid_price_2 = bidPriceList[1]
                tick.bid_price_3 = bidPriceList[2]
                tick.bid_price_4 = bidPriceList[3]
                tick.bid_price_5 = bidPriceList[4]

                tick.bid_volume_1 = bid[tick.bid_price_1]
                tick.bid_volume_2 = bid[tick.bid_price_2]
                tick.bid_volume_3 = bid[tick.bid_price_3]
                tick.bid_volume_4 = bid[tick.bid_price_4]
                tick.bid_volume_5 = bid[tick.bid_price_5]

                ask_keys: float = ask.keys()
                askPriceList: list = sorted(ask_keys)

                tick.ask_price_1 = askPriceList[0]
                tick.ask_price_2 = askPriceList[1]
                tick.ask_price_3 = askPriceList[2]
                tick.ask_price_4 = askPriceList[3]
                tick.ask_price_5 = askPriceList[4]

                tick.ask_volume_1 = ask[tick.ask_price_1]
                tick.ask_volume_2 = ask[tick.ask_price_2]
                tick.ask_volume_3 = ask[tick.ask_price_3]
                tick.ask_volume_4 = ask[tick.ask_price_4]
                tick.ask_volume_5 = ask[tick.ask_price_5]
            except IndexError:
                return

        dt: datetime = datetime.now(UTC_TZ)
        tick.datetime = dt

        self.gateway.on_tick(copy(tick))

    def on_wallet(self, data: dict) -> None:
        """资金更新推送"""
        # Exchange模式
        if not self.margin and str(data[0]) != "exchange":
            return
        # Margin模式
        elif self.margin and str(data[0]) != "margin":
            return

        accountid: str = str(data[1])
        account: AccountData = self.accounts.get(accountid, None)
        if not account:
            account: AccountData = AccountData(
                accountid=accountid,
                gateway_name=self.gateway_name,
            )

        account.balance = float(data[2])
        account.available = 0.0
        account.frozen = 0.0
        self.gateway.on_account(copy(account))

    def on_trade_update(self, data: dict) -> None:
        """成交信息推送"""
        name: str = data[1]
        info: dict = data[2]

        if name == "ws":
            for l in info:
                self.on_wallet(l)
            self.gateway.write_log("账户资金获取成功")
        elif name == "wu":
            self.on_wallet(info)
        elif name == "os":
            for l in info:
                self.on_order(l)
            self.gateway.write_log("活动委托获取成功")
        elif name in ["on", "ou", "oc"]:
            self.on_order(info)
        elif name == "te":
            self.on_trade(info)
        elif name == "n":
            self.on_order_error(info)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """触发异常回调"""
        msg: str = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb)
        )

    def authenticate(self) -> None:
        """用户授权验证"""
        nonce: int = int(time.time() * 1000000)
        authPayload: str = "AUTH" + str(nonce)
        signature: bytes = hmac.new(
            self.secret, authPayload.encode(), digestmod=hashlib.sha384
        ).hexdigest()

        req: dict = {
            "apiKey": self.key,
            "event": "auth",
            "authPayload": authPayload,
            "authNonce": nonce,
            "authSig": signature
        }

        self.send_packet(req)

    def subscribe_topic(self) -> None:
        """订阅Websocket私有频道"""
        req: dict = {
            "op": "subscribe",
            "args": [
                "instrument",
                "trade",
                "orderBook10",
                "execution",
                "order",
                "position",
                "margin",
            ],
        }
        self.send_packet(req)

    def on_trade(self, data: dict) -> None:
        """成交更新推送"""
        self.trade_id += 1

        if data[4] > 0:
            direction = Direction.LONG
        else:
            direction = Direction.SHORT

        trade: TradeData = TradeData(
            symbol=str(data[1].replace("t", "")),
            exchange=Exchange.BITFINEX,
            orderid=data[-1],
            direction=direction,
            volume=abs(data[4]),
            price=data[5],
            tradeid=str(self.trade_id),
            datetime=generate_datetime(data[2]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)

    def on_order_error(self, d: list) -> None:
        """委托更新推送触发异常回报"""
        if d[-2] != "ERROR":
            return

        data: list = d[4]
        error_info: str = d[-1]

        # 过滤撤销或不存在的委托
        orderid: str = str(data[2])
        if orderid == "None":
            self.gateway.write_log("撤单失败，委托不存在")
            return

        if data[6] > 0:
            direction = Direction.LONG
        else:
            direction = Direction.SHORT

        order: OrderData = OrderData(
            symbol=str(data[3].replace("t", "")),
            exchange=Exchange.BITFINEX,
            type=ORDERTYPE_BITFINEX2VT[data[8]],
            orderid=orderid,
            status=Status.REJECTED,
            direction=direction,
            price=float(data[16]),
            volume=abs(data[6]),
            datetime=generate_datetime(d[0]),
            gateway_name=self.gateway_name,
        )

        self.gateway.on_order(copy(order))

        self.gateway.write_log(f"委托拒单：{error_info}")

    def on_order(self, data: dict) -> None:
        """委托更新推送"""
        orderid: str = str(data[2])

        if data[7] > 0:
            direction = Direction.LONG
        else:
            direction = Direction.SHORT

        order_status: str = str(data[13].split("@")[0]).replace(" ", "")
        if order_status == "CANCELED":
            dt: datetime = generate_datetime(data[5])
        else:
            dt: datetime = generate_datetime(data[4])

        order: OrderData = OrderData(
            symbol=str(data[3].replace("t", "")),
            exchange=Exchange.BITFINEX,
            orderid=orderid,
            type=ORDERTYPE_BITFINEX2VT[data[8]],
            status=STATUS_BITFINEX2VT[order_status],
            direction=direction,
            price=float(data[16]),
            volume=abs(data[7]),
            traded=abs(data[7]) - abs(data[6]),
            datetime=dt,
            gateway_name=self.gateway_name,
        )

        self.gateway.on_order(copy(order))


def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = UTC_TZ.localize(dt)
    return dt
