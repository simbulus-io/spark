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
        actual = list(output_df.timestamp.values)
        assert all([str(a) == str(b) for a, b in zip(actual, expected)])
