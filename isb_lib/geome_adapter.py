"""
Implements an adapter for getting records from GEOME
"""
import time
import logging
import functools
import datetime
import urllib.parse
import json
import requests
import sickle.oaiexceptions
import sickle.utils
import igsn_lib.oai
import igsn_lib.time
import igsn_lib.models.thing
import isb_lib.core

HTTP_TIMEOUT = 10.0  # seconds
AUTHORITY_ID = "GEOME"
GEOME_API = "https://api.geome-db.org/v1/"

RELATION_TYPE = {
    "child": "child",
    "parent": "parent",
}

def getLogger():
    return logging.getLogger("isb_lib.geome_adapter")


@functools.cache
def _datetimeFromSomething(tstr):
    return igsn_lib.time.datetimeFromSomething(tstr)


def geomeEventRecordTimestamp(record):
    t_collected = None
    try:
        _year = record.get("yearCollected", "")
        tstr = f"{_year}-01-01"
        if _year != "":
            _month = record.get("monthCollected", "")
            if _month != "":
                tstr = f"{_year}-{_month}"
                _day = record.get("dayCollected", "")
                if _day != "":
                    tstr = f"{_year}-{_month}-{_day}"
                    _tod = record.get("timeOfDay", "")
                    if _tod != "":
                        tstr = f"{tstr} {_tod}"
            t_collected = _datetimeFromSomething(tstr)
            # L.debug("T: %s, %s", tstr, t_collected)
    except Exception as e:
        pass
    return t_collected


class GEOMEIdentifierIterator(isb_lib.core.IdentifierIterator):
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        record_type: str = "Sample"
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
        )
        self._record_type = record_type
        self._project_ids = None

    def listProjects(self):
        L = getLogger()
        L.debug("Loading project ids...")
        url = f"{GEOME_API}projects"
        headers = {
            "Accept": "application/json",
        }
        params = {"includePublic": "true", "admin": "false"}
        response = requests.get(
            url, params=params, headers=headers, timeout=HTTP_TIMEOUT
        )
        if response.status_code != 200:
            raise ValueError(
                "Unable to load projects, status: %s; reason: %s",
                response.status_code,
                response.reason,
            )
        projects = response.json()
        for project in projects:
            L.debug("project id: %s", project.get("projectId", None))
            yield project

    def recordsInProject(self, project_id, record_type):
        L = getLogger()
        L.debug("recordsInProject project %s", project_id)
        url = f"{GEOME_API}records/{record_type}/json"
        headers = {
            "Accept": "application/json",
        }
        _page_size = 1000
        params = {
            "limit": _page_size,
            "page": 0,
            "q": f"_projects_:{project_id}",
        }
        more_work = True
        while more_work:
            response = requests.get(
                url, params=params, headers=headers, timeout=HTTP_TIMEOUT
            )
            if response.status_code != 200:
                L.error(
                    "Unable to load records project:%s; status: %s; reason: %s",
                    project_id,
                    response.status_code,
                    response.reason,
                )
                break
            # L.debug("recordsInProject data: %s", response.text[:256])
            data = response.json()
            for record in data.get("content", {}).get(record_type, []):
                L.debug("recordsInProject Record id: %s", record.get("bcid", None))
                #print(json.dumps(record, indent=2))
                #raise NotImplementedError
                yield record
            if len(data.get("content", {}).get(record_type, [])) < _page_size:
                more_work = False
            params["page"] = params["page"] + 1

    def _loadEntries(self):
        L = getLogger()
        if self._project_ids is None:
            # Load the project IDs
            # each entry is a dict with key
            self._project_ids = []
            for p in self.listProjects():
                pid = p.get("projectId", None)
                if not pid is None:
                    self._project_ids.append(
                        {
                            "project_id": pid,
                            "identifiers": [],
                            "loaded": False,
                        }
                    )
        self._cpage = []
        for p in self._project_ids:
            # return the next set of identifiers within a project
            if not p["loaded"]:
                for record in self.recordsInProject(
                    p["project_id"], self._record_type
                ):
                    # record identifier
                    rid = record.get("bcid", None)
                    # record timestamp
                    t_collected = geomeEventRecordTimestamp(record)
                    if not rid is None:
                        p["identifiers"].append((rid, t_collected))
                p["loaded"] = True
                L.info(
                    "Added %s identifiers from project %s",
                    len(p["identifiers"]),
                    p["project_id"],
                )
                # Make this the next page
                self._cpage = p["identifiers"]
                self._total_records += len(self._cpage)
                # if more than zero records on this page, then break to yield them
                if len(self._cpage) > 0:
                    L.debug(
                        "Breaking on project_id: %s, total_records: %s",
                        p["project_id"],
                        self._total_records,
                    )
                    break

    def __len__(self):
        return self._total_records

    def _getPage(self):
        if self._cpage is None or self._coffset >= self._total_records:
            self._page_offset = 0
            self._loadEntries()
            return


def getGEOMEItem_json(identifier, verify=False):
    headers = {"Accept": "application/json"}
    #
    #url = f"{GEOME_API}records/{urllib.parse.quote(identifier, safe='')}"
    url = f"{GEOME_API}records/{identifier}"
    params = {"includeChildren": "true", "includeParent": "true"}
    res = requests.get(url, headers=headers, params=params, verify=verify, timeout=HTTP_TIMEOUT)
    return res


class GEOMEItem(object):
    def __init__(self, source):
        self.authority_id = AUTHORITY_ID
        self.item = source

    def getRelations(self, tstamp):
        if isinstance(tstamp, datetime.datetime):
            tstamp = igsn_lib.time.datetimeToJsonStr(tstamp)
        related = []
        _id = self.item.get("parent",{}).get("bcid","")
        if _id != "":
            related.append((tstamp, RELATION_TYPE['parent'], _id))
        for child in self.item.get("children", []):
            _id = child.get("bcid")
            if _id != "":
                related.append((tstamp, RELATION_TYPE['child'], _id))
        return related

    def getItemType(self):
        return self.item.get("record", {}).get("entity", None)

    def asThing(
        self,
        identifier: str,
        t_created: datetime.datetime,
        status: int,
        resolved_url: str,
        t_resolved: datetime.datetime,
        resolve_elapsed: float,
        media_type: str,
    ):
        L = getLogger()
        L.debug("GEOMEItem.asThing")
        if t_created is None:
            parent_record = self.item.get("parent", {})
            t_created = geomeEventRecordTimestamp(parent_record)
        _thing = igsn_lib.models.thing.Thing(
            id=identifier,
            tcreated=t_created,
            item_type=None,
            authority_id=self.authority_id,
            resolved_url=resolved_url,
            resolved_status=status,
            tresolved=t_resolved,
            resolve_elapsed=resolve_elapsed,
        )
        if not isinstance(self.item, dict):
            L.error("Item is not an object")
            return _thing
        _thing.item_type = self.getItemType()
        _thing.related = self.getRelations(t_created)
        _thing.resolved_media_type = media_type
        _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.item
        return _thing


def reparseThing(thing):
    """Reparse the resolved_content"""
    if not isinstance(thing.resolved_content, dict):
        raise ValueError("Thing.resolved_content is not an object")
    if not thing.authority_id == AUTHORITY_ID:
        raise ValueError("Thing is not a GEOME item")
    item = GEOMEItem(thing.resolved_content)
    tstamp = igsn_lib.time.datetimeToJsonStr(thing.tcreated)
    thing.related = item.getRelations(tstamp)
    thing.item_type = item.getItemType()
    thing.tstamp = igsn_lib.time.dtnow()
    return thing


def loadThing(igsn_value, t_created):
    L = getLogger()
    L.info("loadThing: %s", igsn_value)
    response = getGEOMEItem_json(igsn_value, verify=True)
    t_resolved = igsn_lib.time.dtnow()
    elapsed = igsn_lib.time.datetimeDeltaToSeconds(response.elapsed)
    for h in response.history:
        elapsed = igsn_lib.time.datetimeDeltaToSeconds(h.elapsed)
    r_url = response.url
    r_status = response.status_code
    media_type = response.headers["content-type"]
    obj = None
    try:
        obj = response.json()
    except Exception as e:
        L.warning(e)
    item = GEOMEItem(obj)
    _thing = item.asThing(
        igsn_value, t_created, r_status, r_url, t_resolved, elapsed, media_type
    )
    return _thing


def reloadThing(thing):
    """Given an instance of thing, reload from the source and reparse."""
    L = getLogger()
    L.debug("reloadThing id=%s", thing.id)
    igsn_value = igsn_lib.normalize(thing.id)
    return loadThing(igsn_value, thing.tcreated)
