# -*- coding: utf-8 -*-
from utils import clean_up_helpers
import pandas as pd


class TestTimestamps:
    events = [
        {"server_timestamp": 1685542526261.0},
        {"timestamp": 1660681782.869},
        {"timestamp": 1.659733e09},
        {"timestamp": 1.662688e09},
    ]

    def test_timestamps(self):
        events_df = pd.DataFrame(self.events)

        output_df = clean_up_helpers.align_timestamps(events_df)
        assert output_df.shape[0] == 4
        expected = [
            "2023-05-31T14:15:26.261000000",
            "2022-08-16T20:29:42.869000000",
            "2022-08-05T20:56:40.000000000",
            "2022-09-09T01:46:40.000000000",
        ]
        actual_utc = list(output_df.timestamp.values)
        assert all([str(a) == str(b) for a, b in zip(actual_utc, expected)])

        expect_et = [
            "2023-05-31T10:15:26.261000-04:00",
            "2022-08-16T16:29:42.869000-04:00",
            "2022-08-05T16:56:40-04:00",
            "2022-09-08T21:46:40-04:00",
        ]
        actual_et = list(output_df.timestamp_tz_et.values)
        assert all([str(a) == str(b) for a, b in zip(actual_et, expect_et)])

        expect_et_date = [
            "20230531",
            "20220816",
            "20220805",
            "20220908",
        ]
        actual_et_date = list(output_df.date_tz_et.values)
        assert all([str(a) == str(b) for a, b in zip(actual_et_date, expect_et_date)])
