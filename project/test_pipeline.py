import os
import pandas as pd
import pytest
import sys

# Add made-template to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/../.."))

from project.pipeline import (
    download_registration_records,
    download_rural,
    download_education,
    download_age_sex,
    download_race,
    download_income,
    create_directories,
    main,
    urls,
)
import shutil


@pytest.fixture
def empty(tmp_path, monkeypatch):
    # Delete all files and directories in the data folder
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setattr("project.pipeline.base_path", str(tmp_path))
    yield tmp_path


@pytest.fixture
def base(tmp_path, monkeypatch):
    # Delete all files and directories in the data folder
    data = tmp_path / "data"
    data.mkdir()
    raw = data / "raw"
    raw.mkdir()
    processed = data / "processed"
    processed.mkdir()
    monkeypatch.setattr("project.pipeline.base_path", str(tmp_path))
    yield tmp_path


def test_directories(empty):
    # Ensure the data directory exists
    create_directories()

    # Check if the directories were created
    assert os.path.exists(empty / "data/raw") == True
    assert os.path.exists(empty / "data/processed") == True

    # Clean up
    os.rmdir(empty / "data/raw")
    os.rmdir(empty / "data/processed")


def test_download_registration_records(base):

    # Test the download function
    download_registration_records(urls["registration_records"])

    # Check if the file was created and is not empty
    assert os.path.exists(base / "data/raw/registration_records.csv") is True
    assert os.path.getsize(base / "data/raw/registration_records.csv") > 0

    assert os.path.exists(base / "data/processed/registration_records.csv") is True
    assert os.path.getsize(base / "data/processed/registration_records.csv") > 0


def test_download_rural(base):
    # Test the download function
    download_rural(urls["rural"])

    # Check if the file was created and is not empty
    assert os.path.exists(base / "data/raw/rural.json") is True
    assert os.path.getsize(base / "data/raw/rural.json") > 0

    assert os.path.exists(base / "data/processed/rural.csv") is True
    assert os.path.getsize(base / "data/processed/rural.csv") > 0


def test_download_education(base):
    # Test the download function
    download_education(urls["education"])

    # Check if the file was created and is not empty
    assert os.path.exists(base / "data/raw/education.json") is True
    assert os.path.getsize(base / "data/raw/education.json") > 0

    assert os.path.exists(base / "data/processed/education.csv") is True
    assert os.path.getsize(base / "data/processed/education.csv") > 0


def test_download_age_sex(base):
    # Test the download function
    download_age_sex(urls["age_sex"])

    # Check if the file was created and is not empty
    assert os.path.exists(base / "data/raw/age_sex.json") is True
    assert os.path.getsize(base / "data/raw/age_sex.json") > 0

    assert os.path.exists(base / "data/processed/age_sex.csv") is True
    assert os.path.getsize(base / "data/processed/age_sex.csv") > 0


def test_download_race(base):
    # Test the download function
    download_race(urls["race"])

    # Check if the file was created and is not empty
    assert os.path.exists(base / "data/raw/race.json") is True
    assert os.path.getsize(base / "data/raw/race.json") > 0

    assert os.path.exists(base / "data/processed/race.csv") is True
    assert os.path.getsize(base / "data/processed/race.csv") > 0


def test_download_income(base):
    # Test the download function
    download_income(urls["income"])

    # Check if the file was created and is not empty
    assert os.path.exists(base / "data/raw/income.json") is True
    assert os.path.getsize(base / "data/raw/income.json") > 0

    assert os.path.exists(base / "data/processed/income.csv") is True
    assert os.path.getsize(base / "data/processed/income.csv") > 0


def test_full_pipeline(base):
    # Run the full pipeline
    main()

    # Check if the final output has the correct length
    df = pd.read_csv(base / "data/results.csv")
    assert len(df) == 62
