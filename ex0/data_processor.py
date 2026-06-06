#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list) and data:
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        items = data if isinstance(data, list) else [data]
        for item in items:
            self._storage.append((self._counter, str(item)))
            self._counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and data:
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        items = data if isinstance(data, list) else [data]
        for item in items:
            self._storage.append((self._counter, item))
            self._counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(key, str) and isinstance(value, str)
                       for key, value in data.items())
        if isinstance(data, list) and data:
            return all(isinstance(item, dict) and
                       all(isinstance(key, str) and isinstance(value, str)
                       for key, value in item.items())
                       for item in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str: str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            log_str = (
                    f"{item.get('log_level', '')}: "
                    f"{item.get('log_message', '')}"
                    )
            self._storage.append((self._counter, log_str))
            self._counter += 1


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    num_processor = NumericProcessor()
    print(f" Trying to validate input '42': {num_processor.validate(42)}")
    print(f" Trying to validate input 'Hello': "
          f"{num_processor.validate('Hello')}")

    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_processor.ingest("foo")
    except ValueError as e:
        print(f" Got exception: {e}")

    num_data = [1, 2, 3, 4, 5]
    print(f" Processing data: {num_data}")
    num_processor.ingest(num_data)
    print(" Extracting 3 values...")
    for i in range(3):
        index, value = num_processor.output()
        print(f" Numeric value {index}: {value}")

    print("\nTesting Text Processor...")
    txt_processor = TextProcessor()
    print(f" Trying to validate input '42': {txt_processor.validate(42)}")

    txt_data = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {txt_data}")
    txt_processor.ingest(txt_data)
    print(" Extracting 1 values...")
    for i in range(1):
        index, value = txt_processor.output()
        print(f" Text value {index}: {value}")

    print("\nTesting Log Processor...")
    log_processor = LogProcessor()
    print(f" Trying to validate input 'Hello': "
          f"{log_processor.validate('Hello')}")

    log_data = [
                {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!!'}
               ]
    print(f" Processing data: {log_data}")
    log_processor.ingest(log_data)
    print(" Extracting 2 values...")
    for i in range(2):
        index, value = log_processor.output()
        print(f" Log entry {index}: {value}")


if __name__ == "__main__":
    main()
