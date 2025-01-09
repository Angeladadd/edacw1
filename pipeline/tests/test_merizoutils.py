import unittest
from unittest import mock
from unittest.mock import MagicMock, patch
import os
import sys
import tempfile
import pandas as pd

# If you need to adjust `sys.path` so this test can import `merizoutils.py`,
# make sure you set the correct directory:
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Adjust these paths as necessary for your environment
python_path = '/home/almalinux/pipeline/venv/bin/python'
merizo_path = '/home/almalinux/merizo_search/merizo_search/merizo.py'
db_path = '/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb'

# Import from your local package (ensure __init__.py is present in 'utils')
from utils.merizoutils import (
    batch_search_and_parse,
    split_search_files,
    split_segment_files
)

class TestMerizoUtils(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.test_dir.cleanup)
        # Example test files you might create
        self.search_file_path = os.path.join(self.test_dir.name, "test_search.tsv")
        self.segment_file_path = os.path.join(self.test_dir.name, "test_segment.tsv")

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
        results = batch_search_and_parse(
            files, 
            s3client, 
            output_bucket,
            python_path=python_path,
            merizo_path=merizo_path,
            db_path=db_path
        )

        # Assert
        mock_batch_write_tmp_local.assert_called_once_with(files, 4)
        mock_run_merizo_search.assert_called_once_with(
            local_dir,
            f"{local_dir}/output",
            python_path,
            merizo_path,
            db_path,
            4
        )
        mock_parse_and_save.assert_called_once_with(
            [f"{local_dir}/output"], 
            s3client, 
            output_bucket
        )
        mock_clean_tmp_local.assert_called_once_with(local_dir)
        self.assertEqual(results, [('file1', 0.95, {'cath1': 2})])

    @patch('utils.merizoutils.run_merizo_search')
    @patch('utils.merizoutils.batch_write_tmp_local')
    @patch('utils.merizoutils.clean_tmp_local')
    @patch('utils.merizoutils.parse_and_save')
    @patch('utils.merizoutils.write_metrics')
    def test_batch_search_and_parse_failure_with_retries(
        self, mock_write_metrics, mock_parse_and_save, 
        mock_clean_tmp_local, mock_batch_write_tmp_local, 
        mock_run_merizo_search
    ):
        # Setup
        files = [('file1.pdb', 'content1'), ('file2.pdb', 'content2')]
        s3client = MagicMock()
        output_bucket = 'test-bucket'
        local_dir = '/tmp/test_dir'
        mock_batch_write_tmp_local.return_value = local_dir

        # We simulate:
        #  1) First batch run fails => return code 1
        #  2) Next call for file1 succeeds => return code 0
        #  3) Next call for file2 fails => return code 1
        #  4) Next retry for file2 succeeds => return code 0
        mock_run_merizo_search.side_effect = [1, 0, 1, 0]

        mock_parse_and_save.return_value = [
            ('file1', 0.95, {'cath1': 2}),
            ('file2', 0.90, {'cath2': 3})
        ]

        # Execute
        results = batch_search_and_parse(
            files, 
            s3client, 
            output_bucket,
            python_path=python_path,
            merizo_path=merizo_path,
            db_path=db_path,
            retry=2
        )

        # Assert
        self.assertEqual(mock_run_merizo_search.call_count, 4)
        expected_calls = [
            # 1) Initial batch call
            mock.call(
                local_dir,
                f"{local_dir}/output",
                python_path,
                merizo_path,
                db_path,
                4
            ),
            # 2) file1 retry call
            mock.call(
                os.path.join(local_dir, 'file1.pdb'),
                f"{local_dir}/file1",
                python_path,
                merizo_path,
                db_path,
                4
            ),
            # 3) file2 retry call 1
            mock.call(
                os.path.join(local_dir, 'file2.pdb'),
                f"{local_dir}/file2",
                python_path,
                merizo_path,
                db_path,
                4
            ),
            # 4) file2 retry call 2
            mock.call(
                os.path.join(local_dir, 'file2.pdb'),
                f"{local_dir}/file2",
                python_path,
                merizo_path,
                db_path,
                4
            ),
        ]
        mock_run_merizo_search.assert_has_calls(expected_calls, any_order=False)

        # parse_and_save should only be called once at the end
        mock_parse_and_save.assert_called_once_with(
            [f"{local_dir}/file1", f"{local_dir}/file2"],
            s3client,
            output_bucket
        )
        mock_clean_tmp_local.assert_called_once_with(local_dir)
        # Since both files eventually succeeded, MERIZO_FAILED_METRIC = 0
        mock_write_metrics.assert_called_once_with('merizo_search_failed', 0)

        self.assertEqual(
            results, 
            [('file1', 0.95, {'cath1': 2}), ('file2', 0.90, {'cath2': 3})]
        )

    def test_split_search_files_ok(self):
        """Check that split_search_files parses a small TSV properly."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as tmp:
            tempname = tmp.name
            df = pd.DataFrame([
                {'query': 'someid_merizo_1', 'dom_plddt': 100, 'metadata': '{"cath":"cath1"}'},
                {'query': 'someid_merizo_2', 'dom_plddt': 80,  'metadata': '{"cath":"cath2"}'},
            ], columns=['query', 'dom_plddt', 'metadata'])
            df.to_csv(tmp, sep='\t', index=False)
        
        try:
            result = split_search_files(tempname)
            self.assertIn('someid', result, "Expected to find 'someid' as grouped ID")

            mean_plddt, cath_map, (filename, file_content) = result['someid']
            # Check computed mean => (100 + 80) / 2 = 90
            self.assertAlmostEqual(mean_plddt, 90.0, places=3)
            # Check that cath1 and cath2 each have a count of 1
            self.assertEqual(cath_map.get('cath1'), 1)
            self.assertEqual(cath_map.get('cath2'), 1)
            # Check that the returned filename is something like "someid_search.tsv"
            self.assertTrue(filename.endswith('_search.tsv'))
            self.assertIn('dom_plddt', file_content, "File content should include 'dom_plddt'")
        finally:
            os.remove(tempname)
    
    def test_split_segment_files_ok(self):
        """Check that split_segment_files parses a small TSV properly."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as tmp:
            tempname = tmp.name
            df = pd.DataFrame([
                {'filename': 'someid', 'othercol': 123},
                {'filename': 'someid', 'othercol': 456}
            ], columns=['filename', 'othercol'])
            df.to_csv(tmp, sep='\t', index=False)
        
        try:
            result = split_segment_files(tempname)
            self.assertIn('someid', result, "Expected to find 'someid' as grouped ID")

            segment_filename, segment_content = result['someid']
            self.assertTrue(segment_filename.endswith('_segment.tsv'))
            self.assertIn('othercol', segment_content, "Segment content should contain 'othercol'")
        finally:
            os.remove(tempname)

if __name__ == '__main__':
    unittest.main()