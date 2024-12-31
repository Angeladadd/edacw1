import unittest
from unittest import mock
from unittest.mock import MagicMock, patch
import os

from utils.merizoutils import (
    batch_search_and_parse
)

class TestMerizoUtils(unittest.TestCase):

    @patch('utils.merizoutils.clean_tmp_local')
    @patch('utils.merizoutils.parse_and_save')
    @patch('utils.merizoutils.run_merizo_search')
    @patch('utils.merizoutils.batch_write_tmp_local')
    def test_batch_search_and_parse_success(
        self, mock_batch_write_tmp_local, mock_run_merizo_search,
        mock_parse_and_save, mock_clean_tmp_local
    ):
        # Setup
        files = [('file1.pdb', 'content1'), ('file2.pdb', 'content2')]
        s3client = MagicMock()
        output_bucket = 'test-bucket'
        local_dir = '/tmp/test_dir'
        mock_batch_write_tmp_local.return_value = local_dir
        mock_run_merizo_search.return_value = 0
        mock_parse_and_save.return_value = [('file1', 0.95, {'cath1': 2})]

        # Execute
        results = batch_search_and_parse(files, s3client, output_bucket)

        # Assert
        mock_batch_write_tmp_local.assert_called_once_with(files, 4)
        mock_run_merizo_search.assert_called_once_with(
            local_dir,
            f"{local_dir}/output",
            "/home/almalinux/pipeline/venv/bin/python",
            "/home/almalinux/merizo_search/merizo_search/merizo.py",
            "/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb",
            4
        )
        mock_parse_and_save.assert_called_once_with([f"{local_dir}/output"], s3client, output_bucket)
        mock_clean_tmp_local.assert_called_once_with(local_dir)
        self.assertEqual(results, [('file1', 0.95, {'cath1': 2})])

    @patch('utils.merizoutils.run_merizo_search')
    @patch('utils.merizoutils.batch_write_tmp_local')
    @patch('utils.merizoutils.clean_tmp_local')
    @patch('utils.merizoutils.parse_and_save')
    def test_batch_search_and_parse_failure_with_retries(
        self, mock_parse_and_save, mock_clean_tmp_local,
        mock_batch_write_tmp_local, mock_run_merizo_search
    ):
        # Setup
        files = [('file1.pdb', 'content1'), ('file2.pdb', 'content2')]
        s3client = MagicMock()
        output_bucket = 'test-bucket'
        local_dir = '/tmp/test_dir'
        mock_batch_write_tmp_local.return_value = local_dir
        # First batch run fails
        mock_run_merizo_search.side_effect = [1, 0, 1, 0]  # Fail first, succeed second
        mock_parse_and_save.return_value = [('file1', 0.95, {'cath1': 2}),
                                           ('file2', 0.90, {'cath2': 3})]

        # Execute
        results = batch_search_and_parse(files, s3client, output_bucket, retry=2)

        # Assert
        self.assertEqual(mock_run_merizo_search.call_count, 4)
        expected_calls = [
            mock.call(
                local_dir,
                f"{local_dir}/output",
                "/home/almalinux/pipeline/venv/bin/python",
                "/home/almalinux/merizo_search/merizo_search/merizo.py",
                "/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb",
                4
            ),
            mock.call(
                os.path.join(local_dir, 'file1.pdb'),
                f"{local_dir}/file1",
                "/home/almalinux/pipeline/venv/bin/python",
                "/home/almalinux/merizo_search/merizo_search/merizo.py",
                "/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb",
                4
            ),
            mock.call(
                os.path.join(local_dir, 'file2.pdb'),
                f"{local_dir}/file2",
                "/home/almalinux/pipeline/venv/bin/python",
                "/home/almalinux/merizo_search/merizo_search/merizo.py",
                "/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb",
                4
            )
        ]
        mock_run_merizo_search.assert_has_calls(expected_calls, any_order=False)
        mock_parse_and_save.assert_called_once_with(
            [f"{local_dir}/file1", f"{local_dir}/file2"],
            s3client,
            output_bucket
        )
        mock_clean_tmp_local.assert_called_once_with(local_dir)
        self.assertEqual(results, [('file1', 0.95, {'cath1': 2}),
                                   ('file2', 0.90, {'cath2': 3})])

if __name__ == '__main__':
    unittest.main()