import os
import json
import unittest

from unittest.mock import patch


os.environ["DATA_API_GROUP"] = "8"

from src.validation import inspect_schema


class SchemaTests(unittest.TestCase):

    def setUp(self):

        self.record = {
            "brokered_by": 1,
            "status": "for_sale",
            "price": 250000,
            "bed": 3,
            "bath": 2,
            "acre_lot": 0.25,
            "street": 100,
            "city": "Miami",
            "state": "Florida",
            "zip_code": "33101",
            "house_size": 1500,
            "prev_sold_date": "2020-01-01"
        }

    def test_json_list(self):

        payload = json.dumps(
            [self.record]
        ).encode()

        with patch(
            "src.validation.read_raw_payload",
            return_value=payload
        ):

            result = inspect_schema("test")

        self.assertEqual(
            result["status"],
            "COLUMNS_OK"
        )

        self.assertEqual(
            result["record_count"], 1
        )

    def test_json_wrapper(self):

        payload = json.dumps({
            "data": [self.record]
        }).encode()

        with patch(
            "src.validation.read_raw_payload",
            return_value=payload
        ):

            result = inspect_schema("test")

        self.assertEqual(
            result["records_path"],
            "$.data"
        )

    def test_missing_price(self):

        record = self.record.copy()
        del record["price"]

        payload = json.dumps(
            [record]
        ).encode()

        with patch(
            "src.validation.read_raw_payload",
            return_value=payload
        ):

            result = inspect_schema("test")

        self.assertIn(
            "price",
            result["missing_columns"]
        )

    def test_empty_batch(self):

        with patch(
            "src.validation.read_raw_payload",
            return_value=b"[]"
        ):

            result = inspect_schema("test")

        self.assertEqual(
            result["status"],
            "EMPTY_BATCH"
        )

    def test_invalid_json(self):

        with patch(
            "src.validation.read_raw_payload",
            return_value=b"invalid"
        ):

            result = inspect_schema("test")

        self.assertEqual(
            result["status"],
            "UNSUPPORTED_FORMAT"
        )


if __name__ == "__main__":
    unittest.main()
