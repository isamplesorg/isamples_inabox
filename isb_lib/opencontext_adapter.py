import datetime
import isb_lib.core

import logging
import requests

HTTP_TIMEOUT = 10.0  # seconds
OPENCONTEXT_API = "https://opencontext.org/subjects-search/.json?add-attribute-uris=1&attributes=obo-foodon-00001303%2Coc-zoo-has-anat-id%2Ccidoc-crm-p2-has-type%2Ccidoc-crm-p45-consists-of%2Ccidoc-crm-p49i-is-former-or-current-keeper-of%2Ccidoc-crm-p55-has-current-location%2Cdc-terms-temporal%2Cdc-terms-creator%2Cdc-terms-contributor&prop=oc-gen-cat-sample-col%7C%7Coc-gen-cat-bio-subj-ecofact%7C%7Coc-gen-cat-object&response=metadata%2Curi-meta&sort=updated--desc"


def get_logger():
    return logging.getLogger("isb_lib.opencontext_adapter")


class OpenContextIdentifierIterator(isb_lib.core.IdentifierIterator):
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
        )

    def records_in_page(self):
        L = get_logger()
        L.debug("records_in_page")
        url = OPENCONTEXT_API
        headers = {
            "Accept": "application/json",
        }
        _page_size = 200
        params = {
            "rows": _page_size,
        }
        more_work = True
        while more_work:
            response = requests.get(
                url, params=params, headers=headers, timeout=HTTP_TIMEOUT
            )
            if response.status_code != 200:
                L.error(
                    "Unable to load records; status: %s; reason: %s",
                    response.status_code,
                    response.reason,
                )
                break
            # L.debug("recordsInProject data: %s", response.text[:256])
            data = response.json()
            for record in data.get("oc-api:has-results", {}):
                L.debug("records_in_page Record id: %s", record.get("uri", None))
                # print(json.dumps(record, indent=2))
                # raise NotImplementedError
                yield record
            if len(data.get("oc-api:has-results", {})) < _page_size:
                more_work = False
            url = data["next-json"]

    def loadEntries(self):
        self._cpage = []
        counter = 0
        for item in self.records_in_page():
            self._cpage.append(item)
            counter += 1
            if counter > self._max_entries:
                break
        self._total_records = len(self._cpage)

    def _getPage(self):
        """Override this method to retrieve a page of entries from the service.

        After completion, self._cpage contains the next page of entries or None if there
        are no more pages available, and self._page_offset is set to the first entry (usually 0)

        """
        if self._cpage is None:
            self.loadEntries()
        if self._coffset >= self._total_records:
            return
        self._page_offset = 0
