from datetime import datetime, timedelta
from math import log
import os
import sys

c_p = os.getcwd()
if c_p not in sys.path:
    sys.path.append(c_p)

import empyrical as ep
import pandas as pd
import numpy as np

from broker import Broker
from stateful_object import StatefulObject
from price_functions import RV, BV


class Asset(StatefulObject):

    def __init__(self,
                 broker=None,
                 name='',
                 _type='stock',
                 side='',
                 ops=None,
                 qty=0,
                 update_qty=True,
                 cache_returns=False):
        '''
        Parameters
        -------
        ops: conn.Conn
        name: str
            - Id of asset, stock ticker symbol or SIC code
        qty: int
        update_qty: bool
            - Whether to check account to update qty attr
        '''
        StatefulObject.__init__(self)

        if not broker:
            self._broker = Broker()
        else:
            self._broker = broker

        self._id = name
        self._qty = qty
        self._update_qty = update_qty
        self._cache_returns = cache_returns

        self._side = side
        self._type = _type

        self._hist_prices = pd.DataFrame()
        self._last = 0.0
        self._value = 0.0
        self._returns = pd.Series()
        self._risk = 0.0
        self._sharpe = 0.0
        self._invsharpe = 0.0
        self._var = 0.0
        self._rv = 0.0
        self._bv = 0.0
        self._jump = 0.0

    def hist_price(self):

        if '_' not in self._kwargs.keys():
            bars = self._broker.hist_price(self, **self._kwargs)
        else:
            bars = self._broker.hist_price(self)

        if isinstance(bars, dict):
            bars = list(bars.values())[0]

        self._hist_prices = bars

        return bars

    def last(self, **kwargs):
        '''
        Returns
        -------
        last: float
        '''
        last = self._broker.last(self, **kwargs)

        self.__log__('Last: %s' % last)

        return last

    def value(self):
        '''
        Get total dollar value of Asset

        Returns
        -------
        value: float
        '''
        last = self._broker.last(self)
        qty = self._broker.qty(self)

        value = last * qty

        self._value = value

        self.__log__('Value: %s' % value)

        return value

    def qty(self):
        '''
        Returns
        -------
        qty: int
        '''
        current = self._broker.qty(self)

        return current

    def returns(self):
        '''
        WARNING: variable on what price used for calc -> ['open', 'high', 'low', 'close']

        Parameters
        -------
        calc: func
            - of return calculations (simple_returns, cum_returns, cagr)
            - really only simple_returns works as expected

        Returns
        -------
        returns: pd.Series
            - index: timestamp
            - values: returns
        '''
        returns = pd.Series()
        price_type = 'close'

        if self._hist_prices.empty:
            self.hist_price()

        hist_prices = self._hist_prices[price_type].astype(float)

        returns = np.log(hist_prices / hist_prices.shift(1)).dropna()

        returns = returns.rename(self._id)

        self._returns = returns

        self.__log__('Returns: len %s' % len(returns))

        return returns

    def risk(self):
        '''
        Annual Volatility
        TODO: decide what risk measure of returns should be

        Returns
        -------
        risk: float
            - annualized volatility
        '''
        risk = 0.0

        if self._id:
            if self._returns.empty:
                returns = self.returns()
            else:
                returns = self._returns
            risk = ep.annual_volatility(returns, period='daily')

        self._risk = risk

        self.__log__('Risk: %s' % risk)

        return risk

    def sharpe(self):
        '''
        TODO use actual risk-free rate
        Returns
        -------
        sharpe: float
        '''
        sharpe = 0.0

        if self._id:
            if self._returns.empty:
                returns = self.returns()
            else:
                returns = self._returns
            sharpe = ep.sharpe_ratio(returns)
            if self.side == 'short':
                sharpe = -sharpe

        self._sharpe = sharpe

        self.__log__('Sharpe: %s' % sharpe)

        return sharpe

    def VaR(self):
        '''
        Returns
        -------
        var: float
        '''
        var = 0.0

        if self._id:
            if self._returns.empty:
                returns = self.returns()
            else:
                returns = self._returns
            try:
                # TODO IndexError: cannot do a non-empty take from an empty axis
                var = ep.value_at_risk(returns)
            except:
                pass

        self._var = var

        self.__log__('VaR: %s' % var)

        return var

    def rv(self):
        '''
        Wrapper
        '''
        rv = 0.0

        if self._id:
            if self._returns.empty:
                returns = self.returns()
            else:
                returns = self._returns
            rv = RV(returns)

        if np.isinf(rv):
            rv = 0.0

        self._rv = rv

        self.__log__('RV: %s' % rv)

        return rv

    def bv(self):
        '''
        Wrapper
        '''
        bv = 0.0

        if self._id:
            if self._returns.empty:
                returns = self.returns()
            else:
                returns = self._returns
            bv = BV(returns)

        if np.isinf(bv):
            bv = 0.0

        self._bv = bv

        self.__log__('BV: %s' % bv)

        return bv

    def jump(self):
        rv = self.rv()
        bv = self.bv()

        try:
            jump = rv - bv
            if np.isnan(jump):
                jump = 0
        except:
            jump = 0

        self._jump = jump

        self.__log__('Jump: %s' % jump)

        return jump

    @property
    def side(self):

        return self._side

    @side.setter
    def side(self, side):

        self._side = side

    def _reset(self):

        self._hist_prices = pd.DataFrame()
        self._returns = pd.Series()

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
        _out = 'Asset: ' + str(self) + ' | ' + msg

        self._broker.log(_out)

