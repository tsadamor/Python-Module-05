#!/usr/bin/env/python3

from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass
        
    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    @abstractmethod
    def output(self) -> tuple[int, str]
        pass


class NumericProcessor(DataProcessor):


class TextProcessor(DataProcessor):


class LogProcessor(DataProcessor):


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")


if __name__ == "__main__":
    main()
