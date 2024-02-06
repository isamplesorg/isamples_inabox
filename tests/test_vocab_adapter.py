from isb_lib.vocabulary import vocab_adapter
from unittest.mock import MagicMock


def test_vocab_adapter():
    root_uri = "root_uri"
    root_label = "root_label"
    mock_term = MagicMock()
    mock_term.uri = root_uri
    mock_term.properties.get.return_value = [root_label]
    mock_repository = MagicMock()
    mock_repository.read.return_value = mock_term
    mock_repository.narrower.return_value = []
    vocab_dict = vocab_adapter.uijson_vocabulary_dict("", mock_repository)
    assert vocab_dict == {root_uri: {"children": [], "label": {"en": root_label}}}
