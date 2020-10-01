from math import ceil
import os
import sys

c_p = os.getcwd()
if c_p not in sys.path:
    sys.path.append(c_p)

import numpy as np


class Weight:

    def __init__(self):
        '''
        '''
        pass

    def weight(self,
               node_size,
               scheme='equal',
               **kwargs):
        '''
        Parameters
        -------
        dropout: float
        node_size: int
        scheme: str

        Returns
        -------
        weights: list
        '''
        weights = []

        name = '_' + scheme + "_weighting"

        if hasattr(self, name):
            weights = getattr(self, name)(node_size, **kwargs)
        else:
            weights = self._equal_weighting(node_size)

        return weights

    def _equal_weighting(self, node_size):
        '''
        Parameters
        -------
        node_size: int

        Returns
        -------
        list
        '''
        return [round(1 / node_size, 4) for _ in range(node_size)]

    def _meancentered_weighting(self, node_size):
        '''
        Parameters
        -------
        node_size: int

        Returns
        -------
        weights: list
        '''
        # TODO weights around the mid are all the same
        weights = []
        mid = 1 / node_size
        side_length = int(node_size / 2)
        step = mid / side_length

        below = np.arange(mid, 0, -step)
        above = np.arange(mid, mid * 2, step)

        if node_size % 2 == 0:
            _weights = below.tolist() + above.tolist()
        elif node_size > 1:
            _weights = below.tolist() + [mid] + above.tolist()
        else:
            _weights = [1.0]

        _weights = sorted(_weights, reverse=True)


        _weights = [round(x, 4) for x in _weights]

        if sum(_weights) <= 1:
            weights = _weights

        return weights

    def _nonlinear_weighting(self, node_size):
        '''
        Parameters
        -------
        node_size: int

        Returns
        -------
        weights: list
        '''
        weights = []

        first = 1 - ((1 - np.e ** -(node_size - 1)) / (np.e - 1))
        others = [np.e ** -(x) for x in range(1, node_size)]

        weights = [first] + others

        return weights

    def _dirichelet_weighting(self, node_size):
        '''
        Parameters
        -------
        node_size: int

        Returns
        -------
        weights: list
        '''
        weights = np.random.dirichlet(np.ones(node_size), size=1)
        weights = weights[0].tolist()
        weights = sorted(weights, reverse=True)

        return weights

    def _dropout_weighting(self, node_size, proportion=0.0):
        '''
        Parameters
        -------
        node_size: int
        proportion: float

        Returns
        -------
        int
            - Num to drop from weights
        '''
        num_keep = 0
        if proportion:
            num_keep = ceil(node_size * proportion)

        keep = self._equal_weighting(num_keep)
        discard = [0.0 for _ in range(node_size - num_keep)]

        return keep + discard

    def _clip_to_one(self, node_size):
        '''
        Parameters
        -------
        node_size: int

        Returns
        -------
        weights: list
        '''
        _weights = np.zeros(node_size)

        weights = [x / sum(_weights) for w in _weights]

        return weights


def asset_zscore(symbol):
    '''
    Parameters
    -------

    Returns
    -------
    z_score: signed float
    z_score_magnitude: unsigned float
    '''
    # TODO get ops
    '''
    asset = Asset(symbol, ops=_ops)
    asset._set_kwargs(lookback=MARKETLOOKBACK)
    returns = asset.returns()

    returns_std = returns.std()
    returns_mean = returns.mean()
    current_return = returns[-1]

    asset._set_kwargs(lookback=MARKETLOOKBACK * 2)
    prev_returns = asset.returns()[:-MARKETLOOKBACK]
    prev_std = prev_returns.std()
    prev_ret_mean = prev_returns.mean()

    z_score = (current_return - returns_mean) / returns_std
    z_score_magnitude = abs(z_score)
    '''
    z_score = 0.0
    z_score_magnitude = 0.0
    return z_score, z_score_magnitude

def hierarchy_influence(sector):
    sectors = {
        'Finance': 'XLF',
        'Technology Services': 'XLK',
        'Utilities': 'XLU',
        'Healthcare': 'XLV',
        'Communications': 'XLC',
        'Consumer Staples': 'XLP',
        'Consumer Discretionary': 'XLY',
        'Industrial': 'XLI',
        'Energy Materials': 'XLE'
    }
    sectors = {}
    z_score, magnitude = asset_zscore()
    market_influence = 0.25
    if magnitude > 2:
        market_influence = 1.0
    elif 1 < magnitude <= 2:
        market_influence = 0.75
    elif 0.5 < magnitude <= 1:
        market_influence = 0.5

    sector_influence = 0.0
    industry_influence = 0.0

    model_weights = {'market': market_influence,
                     'sector': sector_influence,
                     'industry': industry_influence}

