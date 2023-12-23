import logging
from pathlib import Path


def logging_config():
    logging_folder = Path(get_project_root_path(), "proxy_helpers", "downloads")
    logging_folder.mkdir(parents=True, exist_ok=True)

    # Configure the root logger
    logging.basicConfig(
        filename=Path(logging_folder, 'proxy_helpers.log'),  # Global log file name
        level=logging.DEBUG,  # Global log level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def get_project_root_path() -> Path:
    root_dir = Path(__file__).parent.parent
    return root_dir


if __name__ == '__main__':
    print(get_project_root_path())
