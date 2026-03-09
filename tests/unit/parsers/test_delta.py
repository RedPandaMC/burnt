"""Unit tests for Delta Lake _delta_log metadata reader."""

import json

import pytest

from burnt.core.exceptions import ParseError
from burnt.parsers.delta import (
    _parse_partition_columns,
    parse_describe_detail,
    read_delta_log,
)


class TestReadDeltaLog:
    def test_basic_add_two_files(self, tmp_path):
        """Two add actions produce correct total_size and num_files."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        log_file = log_dir / "00000000000000000000.json"
        log_file.write_text(
            '{"add": {"path": "part-001.parquet", "size": 1000}}\n'
            '{"add": {"path": "part-002.parquet", "size": 2000}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_files == 2
        assert result.total_size_bytes == 3000
        assert result.location == str(tmp_path)

    def test_remove_excludes_file(self, tmp_path):
        """A removed file is not counted in totals."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        log_file = log_dir / "00000000000000000000.json"
        log_file.write_text(
            '{"add": {"path": "part-001.parquet", "size": 1000}}\n'
            '{"add": {"path": "part-002.parquet", "size": 2000}}\n'
            '{"remove": {"path": "part-001.parquet"}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_files == 1
        assert result.total_size_bytes == 2000

    def test_num_records_summed_when_all_present(self, tmp_path):
        """num_records is summed when all files have stats."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        log_file = log_dir / "00000000000000000000.json"
        stats1 = json.dumps({"numRecords": 500})
        stats2 = json.dumps({"numRecords": 300})
        log_file.write_text(
            f'{{"add": {{"path": "part-001.parquet", "size": 1000, "stats": {json.dumps(stats1)}}}}}\n'
            f'{{"add": {{"path": "part-002.parquet", "size": 2000, "stats": {json.dumps(stats2)}}}}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_records == 800

    def test_num_records_none_when_some_missing_stats(self, tmp_path):
        """num_records is None when at least one file is missing stats."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        log_file = log_dir / "00000000000000000000.json"
        stats1 = json.dumps({"numRecords": 500})
        log_file.write_text(
            f'{{"add": {{"path": "part-001.parquet", "size": 1000, "stats": {json.dumps(stats1)}}}}}\n'
            '{"add": {"path": "part-002.parquet", "size": 2000}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_records is None

    def test_directory_not_found_raises_parse_error(self, tmp_path):
        """ParseError raised when log_dir does not exist."""
        log_dir = tmp_path / "nonexistent" / "_delta_log"
        with pytest.raises(ParseError, match="Delta log directory not found"):
            read_delta_log(log_dir)

    def test_empty_directory_raises_parse_error(self, tmp_path):
        """ParseError raised when log_dir has no JSON files."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        with pytest.raises(ParseError, match="No JSON files found in Delta log"):
            read_delta_log(log_dir)

    def test_malformed_json_lines_are_skipped(self, tmp_path):
        """Malformed JSON lines are silently skipped."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        log_file = log_dir / "00000000000000000000.json"
        log_file.write_text(
            'not valid json\n{"add": {"path": "part-001.parquet", "size": 500}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_files == 1
        assert result.total_size_bytes == 500

    def test_multiple_log_files_processed_in_order(self, tmp_path):
        """Files from multiple log segments are all processed."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        (log_dir / "00000000000000000000.json").write_text(
            '{"add": {"path": "part-001.parquet", "size": 1000}}\n'
        )
        (log_dir / "00000000000000000001.json").write_text(
            '{"add": {"path": "part-002.parquet", "size": 2000}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_files == 2
        assert result.total_size_bytes == 3000

    def test_num_records_none_when_no_files(self, tmp_path):
        """When all files are removed, num_files=0 and num_records=None."""
        log_dir = tmp_path / "_delta_log"
        log_dir.mkdir()
        log_file = log_dir / "00000000000000000000.json"
        log_file.write_text(
            '{"add": {"path": "part-001.parquet", "size": 1000}}\n'
            '{"remove": {"path": "part-001.parquet"}}\n'
        )
        result = read_delta_log(log_dir)
        assert result.num_files == 0
        assert result.total_size_bytes == 0
        assert result.num_records is None


class TestParseDescribeDetail:
    def test_normal_row_returns_delta_table_info(self):
        """A standard DESCRIBE DETAIL row is parsed correctly."""
        rows = [
            {
                "location": "abfss://container@storage.dfs.core.windows.net/table",
                "sizeInBytes": 5000000,
                "numFiles": 10,
                "partitionColumns": ["year", "month"],
            }
        ]
        result = parse_describe_detail(rows)
        assert result.location == "abfss://container@storage.dfs.core.windows.net/table"
        assert result.total_size_bytes == 5000000
        assert result.num_files == 10
        assert result.partition_columns == ["year", "month"]
        assert result.num_records is None

    def test_empty_rows_raises_parse_error(self):
        """ParseError raised when rows list is empty."""
        with pytest.raises(ParseError, match="DESCRIBE DETAIL returned no rows"):
            parse_describe_detail([])

    def test_missing_optional_fields_use_defaults(self):
        """Missing sizeInBytes and numFiles default to 0."""
        rows = [{"location": "/some/path"}]
        result = parse_describe_detail(rows)
        assert result.total_size_bytes == 0
        assert result.num_files == 0
        assert result.partition_columns == []


class TestParsePartitionColumns:
    def test_list_value_returned_as_list(self):
        """A list input is returned as a list of strings."""
        result = _parse_partition_columns(["year", "month"])
        assert result == ["year", "month"]

    def test_string_value_wrapped_in_list(self):
        """A non-empty string is wrapped in a list."""
        result = _parse_partition_columns("year")
        assert result == ["year"]

    def test_empty_string_returns_empty_list(self):
        """An empty string returns an empty list."""
        result = _parse_partition_columns("")
        assert result == []

    def test_none_returns_empty_list(self):
        """None returns an empty list."""
        result = _parse_partition_columns(None)
        assert result == []

    def test_empty_list_returns_empty_list(self):
        """An empty list returns an empty list."""
        result = _parse_partition_columns([])
        assert result == []

    def test_list_with_non_string_values_converted(self):
        """List values are converted to strings."""
        result = _parse_partition_columns([1, 2])
        assert result == ["1", "2"]
