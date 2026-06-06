#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, Protocol


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

    def get_formatted_name(self) -> str:
        raw_name = self.__class__.__name__
        return raw_name.replace("Processor", " Processor")

    def get_total_processed(self) -> int:
        return self._counter

    def get_remaining_count(self) -> int:
        return len(self._storage)


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

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return

        csv_items = []
        for item in data:
            csv_items.append(item[1])
        csv_str = ",".join(csv_items)
        print("CSV Output:")
        print(csv_str)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return

        json_items = []
        for item in data:
            index = item[0]
            value = item[1]
            element_str = f'"item_{index}": "{value}"'
        json_items.append(element_str)

        json_str = ", ".json_items.join()
        print("JSON Output")
        print(json_str)


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            routed = False
            for proc in self._processors:
                if proc.validate(item):
                    proc.ingest(item)
                    routed = True
                    break

            if not routed:
                print(f"DataStream error - "
                      f"Can't process element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            print(f"{proc.get_formatted_name()}: "
                  f"total {proc.get_total_processed()} items processed, "
                  f"remaining {proc.get_remaining_count()} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:

        for proc in self._processors:
            collected_data: list[tuple[int, str]] = []
            for _ in range(nb):
                if proc.get_remaining_count() == 0:
                    break
                
                item = proc.output()
                if item is not None:
                    collected_data.append(item)

        if collected_data:
            plugin.process_output(collected_data)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()

    print("\nRegistering Processors")
    stream.register_processor(num_proc)
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    data = [
            'Hello world',
            [3.14, -1, 2.71],
            [
                {
                    'log_level': 'WARNING',
                    'log_message': 'Telnet access! Use ssh instead'
                },
                {
                    'log_level': 'INFO',
                    'log_message': 'User wil is connected'
                }
            ],
            42,
            ['Hi', 'five']
            ]

    print(f"\nSend first batch of data on stream: {data}")
    stream.process_stream(data)
    stream.print_processors_stats()

    csv_plugin = CSVExportPlugin()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, csv_plugin)


if __name__ == "__main__":
    main()
