import json
import os
import pickle
import random
import sys
import time
from math import floor

c_p = os.getcwd()
if c_p not in sys.path:
    sys.path.append(c_p)

from kafka import TopicPartition, KafkaConsumer, KafkaProducer

import numpy as np
import pandas as pd

from alpaca import Alpaca
from portfolio import Portfolio
from util import Weight


BROKER = Alpaca()
UPDATEFREQ = 60


class KafkaAgent:

    def __init__(self, kafka_consumer=None, symbols=[]):
        self._assets = symbols
        self._nodestate = {}
        self._kafka = kafka_consumer
        self._broker = BROKER
        self._portfolio = Portfolio(BROKER, positions={s: 'long' for s in symbols})
        self._portfolio.value = self._broker.account_value() - 1000

        self._weights = {}
        self._weight = Weight()

        self._state = pd.DataFrame()
        self._agg = pd.DataFrame()
        self._counts = pd.Series()
        self._standard = pd.DataFrame()

        self._ranks = pd.Series()

        self._broker.cancel_open_orders()

    def handle_msg(self, msg):
        data = msg.value

        symbol = data['symbol']

        if symbol not in self._nodestate.keys():
            data['volume'] = data['volume']['bigInteger'] # TODO
            agg = {'vwap': data['vwap'],
                   'trades': data['trades'],
                   'avgTrade': data['avgTrade'],
                   'dollar': data['dollar'],
                   'volume': data['volume'],
                   'nonjump': data['nonjump'],
                   'jump': data['jump'],
                   'totalchange': 0.0}
            self._nodestate[symbol] = {'current': data, 'count': 1, 'agg': agg}
        else:
            data['volume'] = data['volume']['bigInteger']
            old = self._nodestate[symbol]['agg']
            agg = {k: v + data[k] for k, v in old.items() if k != 'totalchange'}
            new_change = np.log(data['vwap'] / self._nodestate[symbol]['current']['vwap'])
            agg['totalchange']  = old['totalchange'] + new_change
            self._nodestate[symbol]['agg'] = agg
            self._nodestate[symbol]['count'] += 1

        return

    def run(self):

        while True:
            msgs = self._kafka.poll()

            if not msgs:
                continue
            for p in list(msgs.values()):
                for m in p:
                    self.handle_msg(m)

            self._update_state()
            self._rank()
            self._trade()
            print('Finished Run')

            time.sleep(UPDATEFREQ)

    def _update_state(self):
        # Make updates/trades
        counts = {}
        agg = pd.DataFrame()
        current = pd.DataFrame()

        for sym in self._nodestate.keys():
            counts[sym] = self._nodestate[sym]['count']
            _agg = pd.DataFrame(self._nodestate[sym]['agg'], index=[sym])
            _current = pd.DataFrame(self._nodestate[sym]['current'], index=[sym])
            agg = pd.concat([_agg, agg])
            current = pd.concat([_current, current])

        counts = pd.Series(counts)
        standard = pd.DataFrame()
        for c in agg.columns:
            if c == 'trades':
                continue
            standard[c] = agg[c] / counts

        self._state = current
        self._agg = agg
        self._counts = counts
        self._standard = standard

    def _rank(self):
        ranked = pd.Series()
        if not (self._standard.empty and self._state.empty):
            ranks = pd.DataFrame().reindex_like(self._standard)
            cols = ['vwap', 'trades', 'avgTrade', 'dollar', 'volume', 'nonjump', 'jump']

            ranks['vwap'] = (np.log(self._state['vwap'] / self._standard['vwap'])).abs().rank()
            ranks['trades'] = self._standard['avgTrade'].rank()

            ranks['jump'] = (self._state['jump'] / self._standard['jump']).abs().rank()
            ranks['nonjump'] = (self._state['jump'] / self._standard['jump']).abs().rank()

            ranked = ranks.dropna(axis=1).sum(axis=1).rank()

            self._ranks = ranked

        return

    def _trade(self):

        _portfolio = self._portfolio

        ranks = list(self._ranks.sort_values(ascending=False).keys())
        to_cancel = [a for a in self._portfolio._assets if str(a) in ranks]

        self._broker.cancel_open_orders(_assets=to_cancel)

        weighting = 'dirichelet'
        _weights = self._weight.weight(node_size=len(ranks), scheme=weighting)
        to_keep = 10
        _weights = self._weight.weight(node_size=to_keep, scheme=weighting) + [0 for x in _weights[to_keep:]]
        rank_weights = {}

        for sym in ranks:
            _asset = [a for a in self._portfolio._assets if str(a) == sym][0]
            ind = ranks.index(sym)
            rank_weights[_asset] = _weights[ind]

        _portfolio.__log__('Rebalance: Getting New Sizes...')
        _portfolio_value = _portfolio.value


        new_positions = {}
        for _p, new_weight in rank_weights.items():
            new_size, size_diff = self._get_size(_p, new_weight, _portfolio_value)
            new_positions[_p] = [new_size, size_diff]

        _portfolio.__log__('Rebalance: Checking Account...')
        total_size = sum([diff[1] for _p, diff in new_positions.items()])
        selling = [_p._id for _p, diff in new_positions.items() if diff[1] < 0]
        cash = self._broker.cash_value()

        can_trade = bool(total_size <= cash)
        positive_cash = bool(abs(cash) == cash)

        _portfolio.__log__('Rebalance: OrderSize: %s Cash: %s CanTrade: %s PositiveCash: %s' % (total_size,
                                                                                                     cash,
                                                                                                     can_trade,
                                                                                                     positive_cash))

        statuses = []


        # Trade Diffs
        if can_trade and positive_cash:
            for _p, diff in new_positions.items():
                _new_size = diff[0]
                status = self._order_newsize(_p, _new_size)
                statuses.append(status)
        elif selling:
            _portfolio.__log__('Rebalance: No Cash Left To Trade, Only Selling...')
            for _p, diff in new_positions.items():
                _new_size = diff[0]
                status = self._order_newsize(_p, _new_size)
                statuses.append(status)
        else:
            _portfolio.__log__('Rebalance: No Cash Left To Trade And No Positions To Sell')

        successes = sum(statuses)
        fails = len(statuses) - successes

        # Logging
        sides = {_p._id: _p.side for _p in _portfolio._assets}
        print_weights = ''

        for _p, v in rank_weights.items():
            side = _p.side
            v = v * 100
            _s = ' %s: %s @ %0.2f%%,' % (_p._id, side, v)
            print_weights += _s

        weight_sum = sum(_weights)

        _portfolio.__log__('Rebalance: Finished With %s Success %s Fail | Total Weights %0.2f ->%s' % (successes,
                                                                                                            fails,
                                                                                                            weight_sum,
                                                                                                            print_weights))

        return

    def _get_size(self, _node, _weight, _total_value):
        '''
        Parameters
        -------
        _node: Asset
        _weight: float
        _total_value: float

        Returns
        -------
        new_size: float
            - New position dollar size
        change: float
            - Difference between new and old sizes
        '''
        new_size = _weight * _total_value

        old_size = _node.qty() * self._state.loc[str(_node)]['vwap']

        change = new_size - old_size

        return new_size, change

    def _order_newsize(self, _asset, _new_size):
        '''
        Parameters
        -------
        _asset: Asset
        _new_size: float

        Returns
        -------
        success/fail: bool
        '''
        _num = 3
        side = _asset.side

        while _num > 0:
            try:
                last_price = self._broker.last(_asset._id)
                new_qty = floor(_new_size / last_price)
                break
            except Exception as e:
                _asset.__log__('Rebalance: ERROR Last Quote %s with %s Retrying in 3 Seconds...' % (_asset._id, e))
                time.sleep(3)
                _num -= 1
                if _num == 0:
                    _asset.__log__('Rebalance: ERROR Last Quote %s Failed.' % _asset._id)
                    return False
                continue

        old_qty = _asset.qty()

        diff = new_qty - old_qty

        if not diff:
                _asset.__log__('Rebalance: Skipping %s with 0 qty diff' % _asset._id)
                return True

        if side == 'long':
            if diff > 0:
                # accumulate
                lp = last_price
                vw_price = self._state.loc[_asset._id]['vwap']
                last_price = vw_price if vw_price < lp else lp
                if last_price == vw_price:
                    _asset.__log__('Rebalance: Using VWAP price...')
                self._broker.order(_asset=_asset,
                                   _qty=diff,
                                   _side='buy',
                                   _type='limit',
                                   _tif='day',
                                   _limit=last_price)
            elif diff < 0:
                # distribute
                self._broker.order(_asset=_asset,
                                   _qty=abs(diff),
                                   _side='sell',
                                   _type='limit',
                                   _tif='day',
                                   _limit=last_price)
            else:
                return False

        elif side == 'short':
            if diff > 0:
                # accumulate
                lp = last_price
                vw_price = self._state.loc[_asset._id]['vwap']
                last_price = vw_price if vw_price > lp else lp
                if last_price == vw_price:
                    _asset.__log__('Rebalance: Using VWAP price...')
                self._broker.order(_asset=_asset,
                                   _qty=diff,
                                   _side='sell',
                                   _type='limit',
                                   _tif='day',
                                   _limit=last_price)
            elif diff < 0:
                # distribute
                self._broker.order(_asset=_asset,
                                   _qty=abs(diff),
                                   _side='buy',
                                   _type='limit',
                                   _tif='day',
                                   _limit=last_price)
            else:
                _asset.__log__('Rebalance: ERROR %s has no side' % _asset._id)
                return False

        return True


if __name__ == '__main__':

    broker = 'kafka:9092'

    kafkacli = KafkaConsumer(bootstrap_servers=[broker],
                             value_deserializer=lambda msg: json.loads(msg),
                             group_id='agent',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             auto_commit_interval_ms=1000)

    # Trade the generated Flink quotes
    quotes = [x for x in kafkacli.topics() if 'agg-' in x]

    syms = [x.split('-')[-1] for x in quotes]
    kafkacli.subscribe(topics=quotes)

    agent = KafkaAgent(kafkacli, symbols=syms)

    agent.run()
