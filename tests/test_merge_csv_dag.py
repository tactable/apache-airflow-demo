import pytest
import pandas as pd
import json
import os
import sys
from pathlib import Path

# Add both the project root and the dags directory to the Python path
current_dir = Path(__file__).parent
project_root = current_dir.parent
dags_dir = project_root / 'dags'

sys.path.extend([
    str(project_root),
    str(dags_dir)
])

from merge_csv_dag import read_csv_1, read_csv_2, merge_csvs, convert_to_json

from unittest.mock import MagicMock, patch

# Test data
TEST_DATA_1 = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'value1': [100, 200, 300]
})

TEST_DATA_2 = pd.DataFrame({
    'id': [2, 3, 4],
    'name': ['Bob', 'Charlie', 'David'],
    'value2': [20, 30, 40]
})

@pytest.fixture
def mock_task_instance():
    """Create a mock task instance with xcom_push and xcom_pull methods"""
    ti = MagicMock()
    ti.xcom_dict = {}
    
    def mock_xcom_push(key, value):
        ti.xcom_dict[key] = value
    
    def mock_xcom_pull(key):
        return ti.xcom_dict.get(key)
    
    ti.xcom_push = mock_xcom_push
    ti.xcom_pull = mock_xcom_pull
    return ti

@pytest.fixture
def mock_context(mock_task_instance):
    """Create a mock context with task instance"""
    return {
        'ti': mock_task_instance
    }

@patch('pandas.read_csv')
def test_read_csv_1(mock_read_csv, mock_context):
    """Test read_csv_1 task"""
    mock_read_csv.return_value = TEST_DATA_1
    
    read_csv_1(**mock_context)
    
    # Verify the DataFrame was converted to JSON and stored in XCom
    stored_json = mock_context['ti'].xcom_dict['df1']
    restored_df = pd.read_json(stored_json)
    
    pd.testing.assert_frame_equal(restored_df, TEST_DATA_1)

@patch('pandas.read_csv')
def test_read_csv_2(mock_read_csv, mock_context):
    """Test read_csv_2 task"""
    mock_read_csv.return_value = TEST_DATA_2
    
    read_csv_2(**mock_context)
    
    stored_json = mock_context['ti'].xcom_dict['df2']
    restored_df = pd.read_json(stored_json)
    
    pd.testing.assert_frame_equal(restored_df, TEST_DATA_2)

def test_merge_csvs(mock_context):
    """Test merge_csvs task"""
    # Setup input data in XCom
    mock_context['ti'].xcom_push('df1', TEST_DATA_1.to_json())
    mock_context['ti'].xcom_push('df2', TEST_DATA_2.to_json())
    
    merge_csvs(**mock_context)
    
    # Get merged result
    merged_json = mock_context['ti'].xcom_pull('merged_df')
    merged_df = pd.read_json(merged_json)
    
    # Expected merged DataFrame
    expected_df = pd.merge(TEST_DATA_1, TEST_DATA_2, on='id', how='outer')
    expected_df['name'] = expected_df['name_x'].combine_first(expected_df['name_y'])
    expected_df = expected_df.drop(columns=['name_x', 'name_y'])
    
    pd.testing.assert_frame_equal(merged_df, expected_df)

def test_convert_to_json(mock_context, tmp_path):
    """Test convert_to_json task"""
    # Setup merged data in XCom
    merged_df = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'value1': [100, 200, 300, None],
        'value2': [None, 20, 30, 40]
    })
    
    # Store the actual JSON string in XCom
    json_str = merged_df.to_json()
    mock_context['ti'].xcom_push('merged_df', json_str)
    
    # Create a temporary output file
    output_file = tmp_path / "merged.json"
    
    # Mock the output path and ensure directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with patch('merge_csv_dag.OUTPUT_JSON', str(output_file)):
        convert_to_json(**mock_context)
    
    # Verify the file was created and contains the correct data
    assert output_file.exists()
    with open(output_file) as f:
        saved_data = json.load(f)
    assert isinstance(saved_data, list)
    assert len(saved_data) == 4

def test_dag_structure():
    """Test the DAG structure and dependencies"""
    from dags.merge_csv_dag import dag
    
    # Get list of task ids
    task_ids = [task.task_id for task in dag.tasks]
    
    # Verify all tasks are present
    assert set(task_ids) == {
        'read_csv_1',
        'read_csv_2',
        'merge_csvs',
        'convert_to_json'
    }
    
    # Verify task dependencies
    merge_task = dag.get_task('merge_csvs')
    convert_task = dag.get_task('convert_to_json')
    
    # Check upstream tasks of merge_csvs
    assert set(task.task_id for task in merge_task.upstream_list) == {
        'read_csv_1',
        'read_csv_2'
    }
    
    # Check downstream task of merge_csvs
    assert convert_task.task_id in [task.task_id for task in merge_task.downstream_list] 