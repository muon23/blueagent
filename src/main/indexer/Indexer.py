from abc import ABC, abstractmethod


class Indexer(ABC):
    @abstractmethod
    def index(self, **kwargs):
        ...
