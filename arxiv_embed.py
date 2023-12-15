import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

from dotenv import load_dotenv

import faiss
import torch
from InstructorEmbedding import INSTRUCTOR
from langchain.docstore.in_memory import InMemoryDocstore
from langchain.vectorstores import FAISS
from rxiv_types import arxiv_records
from tqdm import tqdm

from polus.plugins.data.rxiv.models import ArxivDocument

print(f"GPU count: {torch.cuda.device_count()}")

logging.basicConfig(
    format="%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv(Path(__file__).parent.joinpath(".env").absolute())

data_path = Path(os.environ["DATA_PATH"])

EMBED_INSTRUCTION = (
    "Represent the science paragraph to retrieve the supporting document."
)
LLM: List[INSTRUCTOR] = [
    INSTRUCTOR("hkunlp/instructor-xl") for _ in range(torch.cuda.device_count())
]

FAISS_COLLECTION = FAISS(
    embedding_function=LLM[0].encode,
    docstore=InMemoryDocstore({}),
    index=faiss.IndexFlatL2(768),
    index_to_docstore_id={},
)


def embed_documents(path: Path, device: str = "cuda:0"):
    records = arxiv_records(str(path.absolute()))

    texts = []
    metadatas = []
    ids = []

    assert records.list_records is not None

    for record in records.list_records.record:
        try:
            extracted = ArxivDocument.from_record(record)
            text, metadata, rid = extracted.text_metadata_id()
            texts.append(text)
            metadatas.append(metadata)
            ids.append(rid)
        except:
            continue

    instructions = [[EMBED_INSTRUCTION, text] for text in texts]
    embeddings = LLM[int(device[-1])].encode(instructions, device=device, batch_size=8)

    text_embeddings = [
        (text, embedding.tolist()) for text, embedding in zip(texts, embeddings)
    ]

    return text_embeddings, metadatas, ids, device


threads = []
free_gpu = None
files = list(data_path.joinpath("arxiv/xml").iterdir())
with ThreadPoolExecutor() as executor:
    for file in tqdm(files, total=len(files)):
        if free_gpu is None:
            threads.append(
                executor.submit(embed_documents, file, f"cuda:{len(threads)}")
            )
        else:
            threads.append(executor.submit(embed_documents, file, free_gpu))
        if len(threads) == len(LLM):
            for thread in as_completed(threads):
                text_embeddings, metadatas, ids, free_gpu = thread.result()

                overlapping = set(ids).intersection(FAISS_COLLECTION.docstore._dict)
                if len(overlapping) > 0:
                    tqdm.write(
                        f"Found overlapping ids, skipping addition of {len(overlapping)} records."
                    )
                    new_text = []
                    new_meta = []
                    new_ids = []

                    for index, ident in enumerate(ids):
                        if ident in overlapping:
                            continue
                        new_text.append(text_embeddings[index])
                        new_meta.append(metadatas[index])
                        new_ids.append(ids[index])

                    text_embeddings = new_text
                    metadatas = new_meta
                    ids = new_ids

                FAISS_COLLECTION.add_embeddings(
                    text_embeddings=text_embeddings, metadatas=metadatas, ids=ids
                )
                threads = [t for t in threads if t is not thread]
                break

    print("Completing final embedding adds...")
    for thread in as_completed(threads):
        text_embeddings, metadatas, ids, free_gpu = thread.result()

        overlapping = set(ids).intersection(FAISS_COLLECTION.docstore._dict)
        if len(overlapping) > 0:
            tqdm.write(
                f"Found overlapping ids, skipping addition of {len(overlapping)} records."
            )
            new_text = []
            new_meta = []
            new_ids = []

            for index, ident in enumerate(ids):
                if ident in overlapping:
                    continue
                new_text.append(text_embeddings[index])
                new_meta.append(metadatas[index])
                new_ids.append(ids[index])

            text_embeddings = new_text
            metadatas = new_meta
            ids = new_ids

        FAISS_COLLECTION.add_embeddings(
            text_embeddings=text_embeddings, metadatas=metadatas, ids=ids
        )

    print("Saving data...")
    FAISS_COLLECTION.save_local(str(data_path.joinpath("arxiv")))
    print("All done!")
