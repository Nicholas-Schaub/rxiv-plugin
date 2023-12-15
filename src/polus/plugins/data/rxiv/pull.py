import logging
from concurrent.futures import ProcessPoolExecutor
from dataclasses import field
from itertools import repeat
from pathlib import Path
from typing import List

import boto3, botocore
from pydantic.dataclasses import dataclass
from tqdm import tqdm
from xsdata_pydantic.bindings import XmlParser

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class ArxivFile:
    class Meta:
        name = "file"

    content_md5sum: str = field(default="", metadata={"type": "Element"})
    filename: str = field(default="", metadata={"type": "Element"})
    first_item: str = field(default="", metadata={"type": "Element"})
    last_item: str = field(default="", metadata={"type": "Element"})
    md5sum: str = field(default="", metadata={"type": "Element"})
    num_items: str = field(default="", metadata={"type": "Element"})
    seq_num: str = field(default="", metadata={"type": "Element"})
    size: str = field(default="", metadata={"type": "Element"})
    timestamp: str = field(default="", metadata={"type": "Element"})
    yymm: str = field(default="", metadata={"type": "Element"})


@dataclass
class ArxivList:
    file: List[ArxivFile] = field(default=list, metadata={"type": "Element"})
    timestamp: str = field(default="", metadata={"type": "Element"})


def download_arxiv_file(out_path: Path, key: str):
    """Download an arxiv file.

    Args:
        out_path: _description_
        key: _description_

    Returns:
        _description_
    """

    # Ensure src directory exists
    out_path = out_path.joinpath(key)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        return False

    s3resource = boto3.resource("s3")

    try:
        s3resource.meta.client.download_file(
            Bucket="arxiv",
            Key=key,  # name of file to download from
            Filename=str(out_path.absolute()),  # path to file to download to
            ExtraArgs={"RequestPayer": "requester"},
        )
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("ERROR: " + key + " does not exist in arxiv bucket")
        return False


def pull_arxiv_source(path: Path):
    """Pull in all arxiv source documents (pdfs, text, etc)."""

    out_path = path.joinpath("arxiv")

    # Download manifest file to current directory
    download_arxiv_file(out_path, "src/arXiv_src_manifest.xml")

    index_path = out_path.joinpath("src/arXiv_src_manifest.xml")

    parser = XmlParser()

    result = parser.parse(index_path, ArxivList)

    with ProcessPoolExecutor() as executor:
        threads = executor.map(
            download_arxiv_file, repeat(out_path), [r.filename for r in result.file]
        )

        for thread in tqdm(threads, total=len(result.file)):
            pass


def pull_arxiv_pdf(path: Path):
    """Pull in all arxiv source documents (pdfs, text, etc)."""

    out_path = path.joinpath("arxiv")

    # Download manifest file to current directory
    download_arxiv_file(out_path, "pdf/arXiv_pdf_manifest.xml")

    index_path = out_path.joinpath("pdf/arXiv_pdf_manifest.xml")

    parser = XmlParser()

    result = parser.parse(index_path, ArxivList)

    with ProcessPoolExecutor() as executor:
        threads = executor.map(
            download_arxiv_file, repeat(out_path), [r.filename for r in result.file]
        )

        for thread in tqdm(threads, total=len(result.file)):
            pass
