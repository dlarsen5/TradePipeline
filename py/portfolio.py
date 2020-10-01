import copy

import empyrical as ep
import pandas as pd
import numpy as np

from asset import Asset
from stateful_object import StatefulObject
from price_functions import VWAP, RV, BV


class Portfolio(StatefulObject):

    def __init__(self,
                 broker=None,
                 asset=Asset,
                 positions={},
                 name='',
                 portfolio_risk=0.0,
                 pool=None,
                 update_qty=True,
                 update_value=False,
                 value=0.0,
                 weights={}):
        '''
        Parameters
        -------
        ops: Broker
        asset: Asset
            - asset object to track in portfolio, defaults to equity Asset
        positions: dict
            - dict of symbols -> side: {'AAPL': 'long'}
        name: str
        portfolio_risk: float
            - percent amount to risk of portfolio dollar size
        pool: multiprocessing.pool.ThreadPool
        update_qty: bool
            - True if position share qty should be updated with account shares
            - False if shares should be static (for modeling positions with shares outstanding)
        value: float
            - initial portfolio value to set
        weights: dict
            - keys: 'asset_symbol'
            - values: float
        '''
        StatefulObject.__init__(self)

        self._broker = broker

        if not broker:
            _assets = {}
        else:
            _assets = self._broker.list_positions()

        self._assets = []

        if positions:
            for pos, side in positions.items():
                if pos in _assets.keys():
                    if isinstance(_assets[pos], dict):
                        # TODO what if float number of shares for Robinhood
                        _shares = int(float(_assets[pos]['quantity']))
                        # TODO find side of position
                        _side = 'long'
                    else:
                        _shares = int(_assets[pos].qty)
                        _side = _assets[pos].side

                    _asset = asset(name=pos,
                                   side=_side,
                                   broker=self._broker,
                                   update_qty=update_qty,
                                   qty=_shares)
                    _asset.side = _side

                    self._assets.append(_asset)
                else:
                    _asset = asset(name=pos,
                                side=side,
                                broker=self._broker,
                                update_qty=update_qty,
                                qty=0)
                    _asset.side = side
                    self._assets.append(_asset)
        else:
            self._assets = {}

        self._id = name
        self._pool = pool
        self._value = value
        self._update_value = update_value

        self._hist_prices = {}
        self._returns = pd.DataFrame()
        self._bv = 0.0
        self._jump = 0.0
        self._risk = 0.0
        self._rv = 0.0
        self._sharpe = 0.0
        self._var = 0.0

        self._risks = {}
        self._vars = {}
        self._sharpes = {}
        self._rvs = {}
        self._bvs = {}
        self._jumps = {}

        if not self._assets:
            divisor = 1.0
        else:
            divisor = len(self._assets)
        self._equal_w = 1.0 / divisor

        self._weights = {str(_p): self._equal_w for _p in self._assets}

        _print_attr = 'Weights: ' + ' '.join(['%s: %s' % (k, v) for k, v in self._weights.items()])
        _print_attr += ' Value: %s' % self._value
        actor_msg = 'INIT: %s' % _print_attr

        if self._broker:
            self.__log__(actor_msg)

    def size(self):
        '''
        Num positions in portfolio

        Returns
        -------
        size: int
        '''
        size = len(self._assets)

        return size

    def hist_price(self):

        hist_prices = self._broker.hist_price(self._assets, **self._kwargs)
        self._hist_prices = hist_prices

        return hist_prices

    def vwap(self):

        bars = self.hist_price()
        vwap = {}

        for sym, data in bars.items():

            _vwap = VWAP(data)
            vwap[sym] = _vwap

        self._vwap = vwap

        return vwap

    def returns(self):
        '''
        Weighted sharpe ratio of positions

        Returns
        -------
        returns: pd.DataFrame
        '''
        returns = pd.DataFrame()

        if not self._hist_prices:
            self.hist_price()

        bars = self._hist_prices
        datas = {}

        _returns = []

        for sym, data in bars.items():
            if not isinstance(data, pd.DataFrame):
                data = pd.DataFrame()
            if not data.empty:
                vwap = VWAP(data)
                _return = np.log(vwap / vwap.shift(1)).dropna()
                _return = _return.rename(sym)
                # TODO sometimes datetime isn't the same for every asset
                _return.index = list(range(len(_return)))
            else:
                _return = pd.Series().rename(sym)
            _returns.append(_return)

        if _returns:
            returns = pd.concat(_returns, axis=1)

        self.__log__('Returns: %s rows' % len(returns))

        self._returns = returns

        return returns

    def sharpe(self):
        '''
        Weighted sharpe ratio of positions

        Returns
        -------
        sharpe: float
        '''
        sharpe = 0.0

        if self._returns.empty:
            self.returns()

        self._sharpes = {}

        def _func(_p):
            _ret = self._returns[_p._id]

            _sharpe = ep.sharpe_ratio(_ret)

            _p.__log__('Sharpe: %s' % _sharpe)
            self._sharpes[_p._id] = _sharpe

            if _p._id not in self._weights.keys():
                weight = 0.0
            else:
                weight = self._weights[_p._id]
            v = _sharpe * weight

            return v

        _sharpe = sum(self._map_func(_func))

        if _sharpe:
            sharpe = _sharpe

        self.__log__('Sharpe: %s' % sharpe)

        self._sharpe = sharpe

        return sharpe

    def risk(self):
        '''
        Weighted risk of positions

        Returns
        -------
        risk: float
        '''
        if self._returns.empty:
            self.returns()

        cov = self.cov()
        self._risks = {}

        def _func(_p):
            _cov = self._cov[_p._id].sum()
            _ret = self._returns[_p._id]

            _risk = ep.annual_volatility(_ret, period='daily')
            risk = _risk + _cov

            _p.__log__('Risk: %s' % risk)
            self._risks[_p._id] = risk


            v = risk * self._weights[_p._id]

            return v

        risks = self._map_func(_func)
        risk = sum(self._map_func(_func))

        self.__log__('Risk: %s' % risk)

        self._risk = risk

        return risk

    def VaR(self):
        '''
        Weighted VaR of positions

        Returns
        -------
        var: float
        '''
        if self._returns.empty:
            self.returns()

        self._vars = {}

        def _func(_p):
            _p._set_kwargs(**self._kwargs)
            _ret = self._returns[_p._id]

            var = ep.value_at_risk(_ret)

            _p.__log__('VaR: %s' % var)
            self._vars[_p._id] = var

            v = var * self._weights[_p._id]

            return v

        var = sum(self._map_func(_func))

        self.__log__('VaR: %s' % var)

        self._var = var

        return var

    def rv(self):
        '''
        Weighted Realized Variation of positions

        Returns
        -------
        rv: float
        '''
        if self._returns.empty:
            self.returns()

        self._rvs = {}

        def _func(_p):
            _p._set_kwargs(**self._kwargs)
            _ret = self._returns[_p._id]

            rv = RV(_ret)
            if np.isinf(rv):
                rv = 0.0

            _p.__log__('RV: %s' % rv)
            self._rvs[_p._id] = rv

            v = rv * self._weights[_p._id]

            return v

        rv = sum(self._map_func(_func))

        self.__log__('RV: %s' % rv)

        self._rv = rv

        return rv

    def bv(self):
        '''
        Weighted Bipower Variation of positions

        Returns
        -------
        _bv: float
        '''
        if self._returns.empty:
            self.returns()

        self._bvs = {}

        def _func(_p):
            _p._set_kwargs(**self._kwargs)
            _ret = self._returns[_p._id]

            bv = BV(_ret)
            if np.isinf(bv):
                bv = 0.0

            _p.__log__('BV: %s' % bv)
            self._bvs[_p._id] = bv

            v = bv * self._weights[_p._id]

            return v

        bv = sum(self._map_func(_func))

        self.__log__('BV: %s' % bv)

        self._bv = bv

        return bv

    def jump(self):
        '''
        Get volatility jumps as difference RV - BV
        '''
        if self._returns.empty:
            self.returns()

        self._jumps = {}

        def _func(_p):
            _ret = self._returns[_p._id]

            rv = RV(_ret)
            if np.isinf(rv):
                rv = 0.0

            bv = BV(_ret)
            if np.isinf(bv):
                bv = 0.0

            jump = rv - bv
            if np.isnan(jump):
                jump = 0

            _p.__log__('Jump: %s' % jump)
            self._jumps[_p._id] = jump

            v = jump * self._weights[_p._id]

            return v

        jump = sum(self._map_func(_func))

        self.__log__('Jump: %s' % jump)

        self._jump = jump

        return  jump

    def corr(self):
        '''
        Get volatility jumps as difference RV - BV
        '''

        if self._returns.empty:
            self.returns()

        returns = self._returns

        corr = returns.corr()
        shape = str(corr.shape)

        self.__log__('Corr Shape: %s' % shape)

        self._corr = corr
        self._corr['time'] = pd.Timestamp.now()

        return  corr

    def cov(self):
        '''
        Get volatility jumps as difference RV - BV
        '''

        if self._returns.empty:
            self.returns()

        returns = self._returns

        cov = returns.cov()

        shape = str(cov.shape)

        self.__log__('Cov Shape: %s' % shape)
        self._cov = cov
        '''
        TODO if last 'time' was less than 5 * broker.updatefreq, return the previous cov matrix, set as self._cov, if self._cov
        '''
        self._cov['time'] = pd.Timestamp.now()

        return  cov

    @property
    def value(self):
        '''
        Value of portfolio

        Returns
        -------
        value: float
        '''
        _value = 0.0

        if self._update_value:
            for _p in self._assets:
                _value += _p.value()
        else:
            _value = self._value

        self.__log__('Value: %s' % _value)
        return self._value

    @value.setter
    def value(self, value):
        self.__log__('New Value: %s' % value)

        self._value = value

    def _reset(self):
        '''
        Reset Cached Data
        '''
        self._hist_prices = {}
        self._returns = pd.DataFrame()

    def _map_func(self, _func):
        '''
        Get sum by mapping func to Portfolio Asset(s)
        Parameters
        -------
        func: callable
            - Asset metric to get sum of

        Returns
        -------
        _sum: numeric
        '''
        _return = []

        if self._assets:
            if self._pool:
                _return = self._pool.map(_func, self._assets)
            else:
                _return = list(map(_func, self._assets))

        return _return

    def __repr__(self):

        return self._id

    def __log__(self, msg=''):
        '''
        Log message
        Parameters
        -------
        msg: str
            - message to log
        '''
        _out = 'Portfolio: ' + str(self) + ' | ' + msg

        self._broker.log(_out)
