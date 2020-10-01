import datetime
import os
import threading
import time

import alpaca_trade_api as tradeapi
import pandas as pd

from broker import Broker


def get_api():

    if 'APCA_KEY' not in os.environ.keys():
        from dotenv import load_dotenv
        load_dotenv()

    base_url = os.getenv('APCA_URL')
    key = os.getenv('APCA_KEY')
    secret = os.getenv('APCA_TOKEN')

    api = tradeapi.REST(key_id=key, secret_key=secret, base_url=base_url)

    return api


class Alpaca(Broker):

    def __init__(self, **kwargs):
        kwargs['conn'] = get_api()
        Broker.__init__(self, **kwargs)

    def run(self, target=()):
        '''
        Execute target callable at freq self.updatefreq with the market
        '''
        # cancel open orders
        self.log("Waiting for market to open...")

        tAMO = threading.Thread(target=self.market_open)
        tAMO.start()
        tAMO.join()
        self.log("Market opened.")

        clock = self._conn.get_clock()
        closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()

        orders = self._conn.list_orders(status="open")
        for order in orders:
            self._conn.cancel_order(order.id)

        while True:
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            if currTime == closingTime:
                break
            else:
                target()
                time.sleep(self.updatefreq)

    def market_open(self):
        isOpen = self._conn.get_clock().is_open
        while(not isOpen):
            clock = self._conn.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            self.log(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self._conn.get_clock().is_open

    def check_asset(self, _asset, if_short=False):

        tradable = False

        if isinstance(_asset, str):
            a_id = _asset
        else:
            a_id = _asset._id

        try:
            asset = self._conn.get_asset(a_id)
            if if_short:
                if asset.tradable and asset.shortable:
                    tradable = True
            else:
                if asset.tradable:
                    tradable = True
        except:
            tradable = False

        return tradable

    def hist_price(self, _asset, period='day', date_range=(), lookback=30, **kwargs):
        '''
        Parameters
        -------
        date_range: tuple
            - (start_date, end_date)
            - start_date: pd.DateTime
        period: str
            - timescale
            - Either 'day' or 'minute', default is 30 days

        Returns
        -------
        bars: pd.DataFrame
            - index: 'day'
            - columns: ['open', 'high', 'low', 'close', 'volume']
        '''
        bars = {}

        if isinstance(_asset, str):
            assets = [_asset]
        else:
            if not isinstance(_asset, list):
                assets = [str(_asset)]
            else:
                assets = [str(a) for a in _asset]

        if kwargs:
            if 'durationStr' in kwargs.keys():
                period = 'day' if 'D' in kwargs['durationStr'] else 'minute'
                kwargs['period'] = period
            if 'barSizeSetting' in kwargs.keys():
                lookback = int(str(kwargs['durationStr']).split(' ')[0])
                kwargs['lookback'] = lookback

        if 'period' not in kwargs.keys():
            kwargs['period'] = 'day'
        if 'lookback' not in kwargs.keys():
            kwargs['lookback'] = 30

        if assets:
            try:
                if date_range:
                    period = kwargs['period']
                    _bars = self._conn.get_barset(assets,
                                                  timeframe=period,
                                                  start=date_range[0],
                                                  end=date_range[1])
                    bars = pd.DataFrame(_bars).T
                    bars.columns = [c[3:] for c in bars.columns]
                else:
                    period = kwargs['period']
                    lookback = kwargs['lookback']
                    _bars = self._conn.get_barset(assets,
                                                  timeframe=period,
                                                  limit=lookback)
                    for a_id, data in _bars.items():
                        prices = _bars[a_id]
                        val = []
                        for _v in prices:
                            date = _v.t
                            _val = {'date': date,
                                    'open': _v.o,
                                    'high': _v.h,
                                    'low': _v.l,
                                    'close': _v.c,
                                    'volume': _v.v}
                            val.append(_val)

                        if val:
                            bars[a_id] = pd.DataFrame(val)

            except Exception as e:
                self.log(str(e))

        return bars

    def last(self, _asset):
        '''
        Get last market {price_type}

        Returns
        -------
        last: float
        '''
        if isinstance(_asset, list):
            self._map(self.hist_price, _asset)
            return

        elif isinstance(_asset, str):
            asset = _asset
        else:
            asset = _asset._id

        last = 0.0

        if asset:
            try:
                _bars = self._conn.get_barset(asset, timeframe='1Min', limit=1)
                if _bars:
                    last = _bars[asset][0].c
            except Exception as e:
                self.log(str(e))

        if isinstance(_asset, str):
            return last
        else:
            _asset._last = last
            return last

    def qty(self, _asset):
        '''
        Returns
        -------
        qty: int
        '''
        if isinstance(_asset, list):
            self._map(self.hist_price, _asset)
            return

        current_qty = 0

        if _asset._update_qty:
            try:
                current_qty = abs(int(self._conn.get_position(_asset._id).qty))
            except:
                pass

        _asset._qty = current_qty

        return current_qty

    def order(self, _asset, _qty, _side, _type, _tif, _limit):
        '''
        Parameters:
        -------
        _asset: Asset
        _qty: int
        _side: str
            - 'buy'/'sell'
        _type: str
            - 'limit'
        _tif: str
            - Time In Force
            - 'day'
        _limit: float
            - Limit Price
        '''
        if isinstance(_asset, str):
            symbol = _asset
        else:
            symbol = _asset._id

        order_params = {'symbol': symbol,
                        'qty': _qty,
                        'side': _side,
                        'type': _type,
                        'tif': _tif,
                        'limit': _limit}

        order_string = " ".join([str(k) + ":" + str(v)
                                 for k, v in order_params.items()])

        if symbol == '_cash':
            self.log("Order | %s status:submitted" % order_string)
            return True

        try:
            self._conn.submit_order(symbol=symbol,
                                    qty=_qty,
                                    side=_side,
                                    type=_type,
                                    time_in_force=_tif,
                                    limit_price=_limit)
            self.log("Order | %s status:submitted" % order_string)

        except Exception as e:

            self.log("Order | %s status:failed reason:%s" % (order_string, e))

    def cancel_open_orders(self, _assets=[]):
        open_orders = {order.symbol: order for order in self._conn.list_orders()}
        open_assets = []

        if _assets:
            for _a in _assets:
                a_id = _a._id
                if a_id in open_orders.keys():
                    open_assets.append(a_id)
                    order_id = open_orders[a_id].id
                    self._conn.cancel_order(order_id)

            if open_assets:
                while True:
                    open_orders = {order.symbol: order for order in self._conn.list_orders()}
                    _still_open = []
                    for a_id in open_assets:
                        if a_id in open_orders.keys():
                            _still_open.append(a_id)
                            order_id = open_orders[a_id].id
                            self._conn.cancel_order(order_id)
                    if not _still_open:
                        break
        else:
            attempts = 3
            try:
                self._conn.cancel_all_orders()
            except Exception as e:
                self.log('Cancel Failed: %s' % e)
                return
            open_orders = self._conn.list_orders()
            while open_orders:
                # TODO what if orders don't get cancelled ever? timeout?
                time.sleep(3)
                self.log('Waiting on all orders to cancel...')
                open_orders = self._conn.list_orders()
                attempts -= 1
                if not attempts:
                    self.log('Error:ClosingAllOrders reason:Timeout')
                    break

    def list_positions(self):

        assets = {}
        _assets = self._conn.list_positions()

        if _assets:
            assets = {a.symbol: a for a in _assets}

        return assets

    def close_position(self, _asset):
        if isinstance(_asset, str):
            a_id = _asset
        else:
            a_id = _asset._id
        if self._conn:
            self._conn.close_position(a_id)

    def close_non_positions(self, _assets=[]):

        acct_positions = self._conn.list_positions()
        _sym = [p.symbol for p in acct_positions]

        assetids = [str(_a) for _a in _assets]

        to_close = [s for s in _sym if s not in assetids]

        if to_close:
            while to_close:
                acct_positions = self._conn.list_positions()

                _sym = [p.symbol for p in acct_positions]

                # TODO handle cash position
                to_close = [s for s in _sym if s not in assetids and 'cash' not in s.lower()]

                if not to_close:
                    break
                else:
                    self.cancel_open_orders()
                    for sym in to_close:
                        self.close_position(sym)
                    self.log('Waiting on all non positions to close...')
                    time.sleep(3)

    def activity(self):

        activities = {}

        if self._conn:
            # TODO idk, add check
            activities = self._conn.get_activities()

        return activities

    def cash_value(self):

        cash = self._conn.get_account().cash

        cash = float(cash)

        return cash

    def account_value(self):
        '''
        Get Account Value
        '''
        value = 0.0
        account = self._conn.get_account()

        if account:
            value = float(account.portfolio_value)

        return value

    def vw_price(self, _asset):

        if isinstance(_asset, str):
            a_id = _asset
        else:
            a_id = _asset._id

        vw = 0.0

        snap = self._conn.polygon.snapshot(a_id)

        vw = snap.__dict__['_raw']['ticker']['day']['vw']

        return vw
