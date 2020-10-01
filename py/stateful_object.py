

class StatefulObject:
    def __init__(self):
        self.__cache = False
        self.__cache_kwargs = False
        self.__kwargs = {}
        self._cache_returns = False

    @property
    def _kwargs(self):
        return self.__kwargs

    def _set_kwargs(self, **kwargs):
        # not setter cuz you can't call it then
        self.__kwargs = kwargs

    @property
    def _cache(self):
        return self.__cache

    @_cache.setter
    def _cache(self, cache=False):
        self.__cache = cache

    @property
    def _cache_kwargs(self):
        return self.__cache_kwargs

    @_cache_kwargs.setter
    def _cache_kwargs(self, cache=False):
        self.__cache_kwargs = cache
