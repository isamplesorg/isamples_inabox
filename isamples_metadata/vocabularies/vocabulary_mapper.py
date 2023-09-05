import os
import csv
from pathlib import Path
from typing import Optional

"""
Note that this module operates on a CSV-derived form of the vocabulary sourced at 
https://github.com/isamplesorg/vocabularies/tree/develop/src
"""

class VocabularyTerm:
    def __init__(self, key: str, label: str, uri: str):
        self.key = key
        self.label = label
        self.uri = uri

    def metadata_dict(self) -> dict[str, str]:
        return {
            "label": self.label,
            "identifier": self.uri
        }


class ControlledVocabulary:
    def __init__(self, file_path: str, uri_prefix: str):
        self.uri_prefix = uri_prefix
        self.vocabulary_terms_by_key = {}
        self.vocabulary_terms_by_label = {}
        with open(file_path, newline="") as csvfile:
            csvreader = csv.reader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
            # skip header
            next(csvreader)
            for row in csvreader:
                # The first column is the term key and has a prefix, which we ignore for the purpose of this API,
                # e.g. mat:anthropogenicmetal -> anthropogenicmetal
                key = row[0].split(":")[1]
                label = row[1]
                uri = os.path.join(uri_prefix, key)
                term = VocabularyTerm(key, label, uri)
                self.vocabulary_terms_by_key[key] = term
                self.vocabulary_terms_by_label[label] = term
                print(f"metadata dict is {term.metadata_dict()}")

    def term_for_key(self, key: str) -> Optional[VocabularyTerm]:
        return self.vocabulary_terms_by_key.get(key, None)

    def term_for_label(self, label: str) -> Optional[VocabularyTerm]:
        return self.vocabulary_terms_by_label.get(label, None)


parent_dir = Path(__file__).parent
MATERIAL_TYPE = ControlledVocabulary(os.path.join(parent_dir, "materialType.txt"), "https://w3id.org/isample/vocabulary/material/0.9/")
SAMPLED_FEATURE = ControlledVocabulary(os.path.join(parent_dir, "sampledfeature.txt"), "https://w3id.org/isample/vocabulary/sampledfeature/0.9")
SPECIMEN_TYPE = ControlledVocabulary(os.path.join(parent_dir, "specimenType.txt"), "https://w3id.org/isample/vocabulary/specimentype/0.9")
