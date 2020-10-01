from multiprocessing.pool import ThreadPool
import abc
import datetime
import logging
import os
import threading
import time

DEBUG = os.getenv('DEBUG')


class Broker(abc.ABC):

    def __init__(self,
                 conn=None,
                 logger=None,
                 stream=None,
                 updatefreq=60):
        self._conn = conn
        self._logger = logger
        self.updatefreq = updatefreq

    def run(self, target=()):
        '''
        Execute target callable at freq self.updatefreq with the market
        '''
        raise NotImplementedError

    def market_open(self):

        raise NotImplementedError

    def hist_price(self, _asset, period='day', date_range=(), lookback=30):
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

        raise NotImplementedError

        if isinstance(_asset, str):
            assets = [_asset]
        else:
            if not isinstance(_asset, list):
                assets = [str(_asset)]
            else:
                assets = [str(a) for a in _asset]

        if assets:
            try:
                if date_range:
                    _bars = {}
                else:
                    _bars = {}

            except Exception as e:
                self.log(str(e))

        return bars

    def last(self, _asset, **kwargs):
        '''
        Get last market {price_type}

        Returns
        -------
        last: float
        '''
        raise NotImplementedError

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
                last = 0.0
            except Exception as e:
                self.log(str(e))

        if isinstance(_asset, str):
            return last
        else:
            _asset._last = last
            return last

    def vw_price(self, _asset, **kwargs):

        raise NotImplementedError

        if isinstance(_asset, str):
            a_id = _asset
        else:
            a_id = _asset._id

        vw = 0.0

        return vw

    def qty(self, _asset):
        '''
        Returns
        -------
        qty: int
        '''
        raise NotImplementedError

        if isinstance(_asset, list):
            self._map(self.hist_price, _asset)
            return

        current_qty = 0

        if _asset._update_qty:
            try:
                current_qty = 0
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
        raise NotImplementedError

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

        raise NotImplementedError

    def list_positions(self):

        raise NotImplementedError

        assets = {}
        _assets = self._conn.positions()

        if _assets:
            assets = {a.symbol: a for a in _assets}

        return assets

    def close_position(self, _asset):

        raise NotImplementedError

    def close_non_positions(self, _assets=[]):

        raise NotImplementedError

    def activity(self):

        activities = {}

        return activities

    def cash_value(self):

        cash = 0.0

        return cash

    def account_value(self):

        value = 0.0

        return value

    def log(self, msg=''):

        if not msg:
            return

        if DEBUG:
            ts = time.time()
            timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            print(timestamp + ' ' + msg)

        if self._logger:
            self._logger.info(msg)

    def _map(self, _func, _assets):
        '''
        Apply function to list[Asset]
        '''
        if _assets:
            for _a in _assets:
                _func(_a)

    def __repr__(self):

        return str(self._logger)
