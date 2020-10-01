import numpy as np
import pandas as pd

CONST_U = (np.sqrt(np.pi / 2) ** -2)

def log_returns(prices):
    '''
    Log returns of n and n+1

    Parameters
    -------
    prices: array-like

    Returns
    -------
    returns: numpy.array
    '''
    shifted = zip(prices[1:], prices)
    log_returns = [np.log(x) - np.log(x_) for x, x_ in shifted]

    return log_returns

def RV(returns, normalize=False):
    '''
    Realized Volatility

    Parameters
    -------
    returns: array-like

    Returns
    -------
    RV: numpy.array
    '''
    RV = [x ** 2 for x in returns][1:]
    RV = sum(RV)
    if normalize:
        RV = np.log(RV)

    return RV

def BV(returns, normalize=False):
    '''
    Bi-Power Variation

    Parameters
    -------
    returns: array-like

    Returns
    -------
    BV: numpy.array
    '''
    shifted = zip(returns[2:], returns)
    BV = CONST_U * sum([abs(x) * abs(x_) for x, x_ in shifted])
    if normalize:
        BV = np.log(BV)

    return BV

def sigmoid(x):
    '''
    Scale input to range 0-1
    '''
    def _func(_x):
        return 1 / (1 + np.exp(-_x))

    l_types = (list, np.ndarray, pd.Series)

    if isinstance(x, l_types):
        return [_func(_x) for _x in x]
    else:
        return _func(x)

def softmax(x):
    '''
    Scale input to range 0-1
    '''
    l_types = (list, np.ndarray, pd.Series)

    if not isinstance(x, l_types):
        return []
    else:
        if not isinstance(x, np.ndarray):
            x = np.array(x)
        return (np.e ** x) / np.sum(np.e ** x, axis=0)

def VWAP(prices):
    '''
    Volume Weighted Average Price

    price = mean(open, high, low, close)
    vp = volume * price
    vw = sum(vp) / sum(volume)
    '''
    vwap = pd.Series()

    if 'date' not in prices.columns:
        prices['date'] = prices.index

    prices['date'] = pd.to_datetime(prices['date'])
    prices = prices.set_index('date')
    prices = prices.sort_index(ascending=True)

    prices['wp'] = prices[['open', 'high', 'low', 'close']].astype(float).mean(axis=1)
    prices['vw'] = prices['volume'].astype(int) * prices['wp']

    sumvm = prices.groupby('date')['volume'].sum()
    vwdf = prices.groupby('date')['vw'].sum()

    vwap = vwdf / sumvm

    vwap = vwap.rename('vwap')

    return vwap
