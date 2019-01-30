import unittest
from packer.table_transformations.utils import filter_rows
import pandas as pd
import pandas.testing as pdt


class UtilsTestCase(unittest.TestCase):

    def test_filter_rows_with_same_size_indx(self):
        # given
        src_df = pd.DataFrame([
            ['A', 1],
            ['B', 2],
            ['C', 3],
        ], columns=['letter', 'number'])
        src_df = src_df.set_index(['letter'])

        filter_row_df = pd.DataFrame([
            ['A', 'I'],
            ['C', 'III'],
        ], columns=['letter', 'roman number'])
        filter_row_df = filter_row_df.set_index(['letter'])

        # when
        fitlered_df = filter_rows(src_df, filter_row_df)

        # then
        expected_df = pd.DataFrame(
            [
                ['A', 1],
                ['C', 3],
            ],
            columns=['letter', 'number'])
        expected_df = expected_df.set_index(['letter'])
        pdt.assert_frame_equal(fitlered_df, expected_df)

    def test_filter_rows_with_narrower_filter_indx(self):
        # given
        src_df = pd.DataFrame([
            ['A', 'R1', 1],
            ['B', 'R2', 2],
            ['C', 'R3', 3],
        ], columns=['letter', 'row', 'number'])
        src_df = src_df.set_index(['letter', 'row'])

        filter_row_df = pd.DataFrame([
            ['A', 'I'],
            ['C', 'III'],
        ], columns=['letter', 'roman number'])
        filter_row_df = filter_row_df.set_index(['letter'])

        # when
        fitlered_df = filter_rows(src_df, filter_row_df)

        # then
        expected_df = pd.DataFrame(
            [
                ['A', 'R1', 1],
                ['C', 'R3', 3],
            ],
            columns=['letter', 'row', 'number'])
        expected_df = expected_df.set_index(['letter', 'row'])
        pdt.assert_frame_equal(fitlered_df, expected_df)

    def test_filter_rows_with_wider_filter_indx(self):
        # given
        src_df = pd.DataFrame([
            ['A', 1],
            ['B', 2],
            ['C', 3],
        ], columns=['letter', 'number'])
        src_df = src_df.set_index(['letter'])

        filter_row_df = pd.DataFrame([
            ['A', 'R1', 'I'],
            ['C', 'R2', 'III'],
        ], columns=['letter', 'row', 'roman number'])
        filter_row_df = filter_row_df.set_index(['letter', 'row'])

        # when
        fitlered_df = filter_rows(src_df, filter_row_df)

        # then
        expected_df = pd.DataFrame(
            [
                ['A', 1],
                ['C', 3],
            ],
            columns=['letter', 'number'])
        expected_df = expected_df.set_index(['letter'])
        pdt.assert_frame_equal(fitlered_df, expected_df)

    def test_filter_no_index_columns_in_common(self):
        # given
        src_df = pd.DataFrame([
            ['A', 1],
            ['B', 2],
            ['C', 3],
        ], columns=['letter', 'number'])
        src_df = src_df.set_index(['letter'])

        filter_row_df = pd.DataFrame([
            [1, 'I'],
            [2, 'III'],
        ], columns=['id', 'roman number'])
        filter_row_df = filter_row_df.set_index(['id'])

        # when
        # then
        with self.assertRaises(ValueError) as err:
            filter_rows(src_df, filter_row_df)

        self.assertEqual(str(err.exception), 'No index columns in common to filter rows.')


if __name__ == '__main__':
    unittest.main()
