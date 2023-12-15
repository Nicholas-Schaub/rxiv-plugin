import logging
import os
import tarfile
from pathlib import Path

from dotenv import load_dotenv

from polus.plugins.data.rxiv import fetch_arxiv_xml, pull_arxiv_source, pull_arxiv_pdf

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv(Path(__file__).parent.joinpath(".env").absolute())

data_path = os.environ["DATA_PATH"]

# fetch_arxiv_xml("arxiv", Path(data_path).absolute())

pull_arxiv_source(Path(data_path).absolute())

pull_arxiv_pdf(Path(data_path).absolute())

# tar_path = Path(data_path).joinpath("arxiv/src")
# zip_path = tar_path.parent.joinpath("pdf")
# zip_path.mkdir(exist_ok=True)

# for path in tar_path.iterdir():
#     with tarfile.open(path) as tf:
#         files = tf.getnames()
#         if zip_path.joinpath(files[0]).exists():
#             logger.info(f"Already processed: {zip_path.joinpath(files[0])}")
#             break
#             continue
#         tf.extractall(zip_path)
#         for file in files:
#             out_path = zip_path.joinpath(file)
#             if out_path.name.endswith(".pdf") or out_path.is_dir():
#                 continue
#             with tarfile.open(out_path, "r:gz") as gz:
#                 try:
#                     gz.extractall(out_path.with_name(out_path.name.replace(".gz", "")))
#                 except tarfile.ReadError:
#                     logger.error(f"Could not extract file: {out_path}")
#     break
