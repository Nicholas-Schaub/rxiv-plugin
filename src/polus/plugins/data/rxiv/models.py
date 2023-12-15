from typing import List, Optional

from pydantic import BaseModel
from rxiv_types.arxiv import ArxivRecord


class ArxivDocument(BaseModel):
    rid: str
    title: str
    abstract: str
    authors: List[str]
    year: Optional[str] = None
    month: Optional[str] = None
    day: Optional[str] = None
    link: Optional[str] = None

    @classmethod
    def from_record(cls, record: ArxivRecord):
        assert record.metadata is not None
        assert record.metadata.dc is not None
        dates = record.metadata.dc.date

        if len(dates) > 0:
            year = dates[-1].split("-")[0]
            month = dates[-1].split("-")[1]
            day = dates[-1].split("-")[2].split("T")[0]
        else:
            year = None
            month = None
            day = None

        assert record.header is not None
        assert record.header.identifier is not None
        rid = record.header.identifier.split(":")[-1] + f"v{len(dates)}"

        assert record.metadata.dc.description is not None
        abstract = record.metadata.dc.description[0].replace("\n", " ")

        authors = record.metadata.dc.creator

        title = record.metadata.dc.title[0].replace("\n", "")

        link = None
        for identifier in record.metadata.dc.identifier:
            if identifier.startswith("http://arxiv.org/abs/"):
                link = identifier

        return cls(
            rid=rid,
            title=title,
            abstract=abstract,
            authors=authors,
            year=year,
            month=month,
            day=day,
            link=link,
        )

    def text_metadata_id(self):
        metadata = {
            "title": self.title,
            "authors": self.authors,
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "link": self.link,
            "rid": self.rid,
        }
        return (self.abstract, metadata, self.rid)
