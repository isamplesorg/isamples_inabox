// Default starting query
const DEFAULT_Q = "*:*";

// Field for sample point representation in Solr
const PT_FIELD = "producedBy_samplingSite_location_ll";

// Field for sample temporal range
const DT_FIELD = "producedBy_resultTime";

const DEFAULT_HISTOGRAM_BINS = 35;

// Missing number value
const MISSING_VALUE = "-9999";

// General format for displaying numbers
const FORMAT = "0,0";

// Mouse over hover time before action
const HOVER_TIME = 500; //milliseconds

const DEBOUNCE_TIME = 300;
/**
 * Year length is the average per 400 year cycle.
 * Richards, E. G. (2013), "Calendars", in Urban, S. E.; Seidelmann, P. K. (eds.),
 * Explanatory Supplement to the Astronomical Almanac (3rd ed.),
 * Mill Valley CA: University Science Books, p. 598, ISBN 9781891389856
 */
const DURATION = [
    {v: Math.round(365.2425 * 24.0 * 60.0 * 60.0 * 1000.0), n: "YEAR"},
    {v: 24 * 60 * 60 * 1000, n: "DAY"},
    {v: 60 * 60 * 1000, n: "HOUR"},
    {v: 60 * 1000, n: "MINUTE"},
    {v: 1000, n: "SECOND"}
]


// For escaping solr query terms
const SOLR_RESERVED = [' ', '+', '-', '&', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\'];
const SOLR_VALUE_REGEXP = new RegExp("(\\" + SOLR_RESERVED.join("|\\") + ")", "g");

// Used for keeping track of windows opened by app
var _windows = {};

/**
 * Escape a lucene / solr query term
 */
function escapeLucene(value) {
    return value.replace(SOLR_VALUE_REGEXP, "\\$1");
}

/**
 *  Format a number
 */
function nFormat(v) {
    if (v === undefined) {
        return "";
    }
    if (v === MISSING_VALUE) {
        return v;
    }
    return numeral(v).format(FORMAT);
}

function isNumber(v) {
    return Object.prototype.toString.call(v) === '[object Number]'
}

function isString(v) {
    return Object.prototype.toString.call(v) === '[object String]'
}

function isArray(v) {
    return Object.prototype.toString.call(v) === '[object Array]'
}

function isDate(v) {
    return Object.prototype.toString.call(v) === '[object Date]'
}

function toDate(v) {
    if (isDate(v)) {
        return v;
    }
    return new Date(Date.parse(v));
}

function clipFloat(v, _min, _max) {
    if (v < _min) {
        return _min;
    }
    if (v > _max) {
        return _max;
    }
    return v;
}

function boundsToFQ(bb) {
    if (bb === undefined) {
        return "";
    }
    return PT_FIELD + ":["
        + clipFloat(bb.south, -90.0, 90.0)
        + "," + clipFloat(bb.west, -180.0, 180.0)
        + " TO " + clipFloat(bb.north, -90.0, 90.0)
        + "," + clipFloat(bb.east, -180.0, 180.0) + "]";
}

class SpatialBounds {

    constructor(field = PT_FIELD) {
        this.min_latitude = -90.0;
        this.max_latitude = 90.0;
        this.min_longitude = -180.0;
        this.max_longitude = 180.0;
        this.setWSEN(this.min_longitude, this.min_latitude, this.max_longitude, this.max_latitude);
        this.field = field;
    }

    setWSEN(west, south, east, north) {
        this.west = clipFloat(west, this.min_longitude, this.max_longitude);
        this.south = clipFloat(south, this.min_latitude, this.max_latitude);
        this.east = clipFloat(east, this.min_longitude, this.max_longitude);
        this.north = clipFloat(north, this.min_latitude, this.max_latitude);
    }

    setBounds(bounds) {
        return this.setNSEW(bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth());
    }

    asQuery() {
        return `${this.field}:[${this.south},${this.west} TO ${this.north},${this.east}]`;
    }

    asGeoJSON() {
        return {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [this.west, this.south],
                    [this.east, this.south],
                    [this.east, this.north],
                    [this.west, this.north],
                    [this.west, this.south]
                ]]
            }
        };
    }

}

class TemporalBounds {
    constructor(field = DT_FIELD) {
        this.field = field;
    }

    setRange(t0, t1) {
        this.t0 = toDate(t0);
        this.t1 = toDate(t1);
    }

    /**
     * Solr temporal period for num_bins of the temporal range.
     *
     * e.g. "+10YEAR"
     *
     * @param num_bins
     */
    facetGap(num_bins = DEFAULT_HISTOGRAM_BINS) {
        const delta = (this.t1 - this.t0) / num_bins;
        for (var i = 0; i < DURATION.length; i++) {
            let dt = Math.round(delta / DURATION[i].v);
            if (dt >= 1) {
                return `+${dt}${DURATION[i].n}`;
            }
        }
        return "+1YEAR";
    }

    /**
     * Temporal range as Solr query string
     *
     * @returns {string}
     */
    asQuery() {
        return `${this.field}:[${escapeLucene(this.t0.toISOString())} TO ${escapeLucene(this.t0.toISOString())}]`
    }

    asJsonFacet(num_bins = DEFAULT_HISTOGRAM_BINS) {
        return {
            categories: {
                "type": "range",
                "field": this.field,
                "start": this.t0.toISOString(),
                "end": this.t1.toISOString(),
                "gap": this.facetGap(num_bins)
            }
        }
    }

    /**
     * Generate a histogram of counts for this.temporal_bounds
     *
     * @param q Query, if null then this.q is used
     * @param fq Filter queries, if null then this.fq is used
     * @param num_bins Number of histogram bins
     * @param service Service URL
     * @returns {Promise<{num_docs: *, x: *[], y: *[]}>}
     */
    async getHistogram(q = null,
                       fq = null,
                       num_bins = DEFAULT_HISTOGRAM_BINS,
                       service = "/thing/select") {
        let _url = new URL(service, document.location);
        let query = {
            query: q,
            limit: 0,
            facet: this.asJsonFacet(num_bins)
        };
        let params = _url.searchParams;
        if (isString(fq)) {
            //params.append("fq", fq);
            query.filter = fq;
        } else if (isArray(fq)) {
            query.filter = fq.slice();
            //fq.forEach(v => {
            //    params.append("fq", v);
            //})
        }
        let response = await fetch(_url, {
            method: "POST",
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(query)
        });
        let jdata = await response.json();
        let data = {
            x: [],
            y: [],
            num_docs: jdata.response.numFound
        }
        jdata.facets.categories.buckets.forEach(entry => {
            let v = entry.val.split("T");
            data.x.push(v[0]);
            data.y.push(entry.count);
        })
        return data;
    }


}

/**
 * Gather some basic info about the repo.
 */
function siteInfo() {
    return {
        _info: "",
        init() {
            const url = "https://api.github.com/repos/isamplesorg/isamples_inabox/commits/develop";
            fetch(url)
                .then(response => response.json())
                .then(data => {
                    let _sha = data.sha.substr(0, 7);
                    var dmod = data.commit.author.date;
                    this._info = "Revision " + _sha + " at " + dmod;
                })
        }
    }
}


/**
 * IndexState provides a global view of the index queries that define the current view state.
 */
class IndexState {
    constructor(default_query = DEFAULT_Q) {
        console.log("IndexState.constructor()");
        this.q = default_query;
        this.fq = [];
        this.temporal_bounds = new TemporalBounds();
        this.spatial_bounds = new SpatialBounds();
    }

    setQ(q) {
        if (q === undefined || q === null || q === "") {
            q = DEFAULT_Q;
        }
        this.q = q;
    }

    setFQ(fq) {
        if (fq === undefined || fq === null || fq === "") {
            this.fq = [];
            return;
        }
        if (isString(fq)) {
            this.fq = [fq];
        } else if (isArray(fq)) {
            this.fq = fq.slice();
        }
    }

    setQueries(q, fq) {
        this.setQ(q);
        this.setFQ(fq);
    }

    getQueries(include_spatial=true, include_temporal=true) {
        let res = {
            q: this.q
        };
        let _fq = [];
        if (isArray(this.fq)) {
            _fq = this.fq.slice();
        } else if (isString(this.fq)) {
            _fq = [this.fq];
        }
        if (include_spatial) {
            _fq.push(this.spatial_bounds.asQuery());
        }
        if (include_temporal) {
            _fq.push(this.temporal_bounds.asQuery());
        }
        res.fq = _fq;
        return res;
    }

    async setTemporalBounds(start_date, end_date) {
        this.temporal_bounds.setRange(start_date, end_date);
    }

    async setSpatialBoundsWSEN(west, south, east, north) {
        this.spatial_bounds.setWSEN(west, south, east, north);
    }

    async setSpatialBounds(bounds) {
        this.spatial_bounds.setBounds(bounds);
    }

    async getTemporalHistogram(include_spatial=true,
                               num_bins = DEFAULT_HISTOGRAM_BINS,
                               service = "/thing/select") {
        const queries = this.getQueries(include_spatial, false);
        return this.temporal_bounds.getHistogram(queries.q, queries.fq, num_bins, service);
    }

}

const EV_QUERY_CHANGED = "query-changed";
const EV_SPATIAL_BB_CHANGED = "spatial-bb-changed";
const EV_TEMPORAL_BOUNDS_CHANGED = "temporal-bounds-changed";

function notifyChange(event_name, data) {
    let e = new CustomEvent(event_name, {
        detail: data
    });
    window.dispatchEvent(e);
}

/**
 * Custom event for changes to query or filter
 */
function notifyQueryChanged(query, fquery) {
    notifyChange(EV_QUERY_CHANGED, {q:query, fq:fquery});
}

/**
 * Custom event for changes to spatial bounds
 */
function notifySpatialBoundsChanged(bounds) {
    notifyChange(EV_SPATIAL_BB_CHANGED, {bounds: bounds});
}

function notifyTemporalBoundsChanged(tmin, tmax) {
    notifyChange(EV_TEMPORAL_BOUNDS_CHANGED, {t0: tmin, t1:tmax});
}


/**
 * Get page URL query parameters.
 *
 * Example:
 *   _params = getPageQueryParams();
 *   query = _params.q || "*:*";
 */
function getPageQueryParams() {
    const _qry = new URLSearchParams(window.location.search);
    return Object.fromEntries(_qry.entries());
}

const MSG_SET_QUERY = "set_query";
const MSG_SET_SPATIAL_BOUNDS = "set_spatial_bounds";
const MSG_SET_TEMPORAL_BOUNDS = "set_temporal_bounds";

/**
 * Create an instance of MessageHandler in each window of the application.
 */
class MessageHandler {
    constructor() {
        // actions is a dict of message_name:[list of functions]
        this.actions = {};

        // Windows to broadcast messages to
        this.windows = [];
    }

    addAction(message, action) {
        if (this.actions[message] === undefined) {
            this.actions[message] = [];
        }
        this.actions[message].push(action);
    }

    clearMessageActions(message) {
        this.actions[message] = [];
    }

    /**
     * Listens for messages in the current window and performs actions.
     */
    install() {
        let _this=this;
        window.addEventListener("message", (e, {}) => {
            if (e.origin !== window.location.origin) {
                console.error(`"Mysterious message from: ${e.origin}`);
                return;
            }
            if (e.data !== undefined) {
                if (e.data.name !== undefined) {
                    let _actions = _this.actions[e.data.name];
                    if (_actions !== undefined) {
                        _actions.forEach(_a => _a(e.data));
                    }
                }
            }
        })
    }


}


/**
 * Listen for broadcast messages, possibly from other windows and
 * fire the corresponding event.
 */
function listenForBroadcasts() {
    window.addEventListener("message", (e) => {
        if (e.origin !== window.location.origin) {
            console.log(`Unhandled event from: ${e.origin}`);
            return;
        }
        if (e.data !== undefined) {
            console.log("MESSAGE SOURCE: " + e.source.name);
            console.log("MESSAGE: " + e.data.name);
            if (e.data.name === "set_query") {
                notifyQueryChanged(e.data.q, e.data.fq);
            } else if (e.data.name === "set_spatial_bounds") {
                notifySpatialBoundsChanged(e.data.bounds);
            }
        }
    }, false);
}

function routeMessages() {
    window.addEventListener("message", (e) => {
        if (e.origin !== window.location.origin) {
            console.log(`Unhandled event from: ${e.origin}`);
            return;
        }
        if (e.data !== undefined) {
            broadcastMessage(e.data);
        }
    }, false);

}


function openMapWindow() {
    if (_windows["map"] !== undefined) {
        if (!_windows["map"].closed) {
            return _windows["map"];
        }
    }
    const window_features = "menubar=yes,location=yes,resizable=yes,scrollbars=yes,status=yes";
    _windows["map"] = window.open("/map", window.parent.name + "spatial", window_features);
}

function openRecordsWindow() {
    if (_windows["records"] !== undefined) {
        if (!_windows["records"].closed) {
            return _windows["records"];
        }
    }
    const window_features = "menubar=yes,location=yes,resizable=yes,scrollbars=yes,status=yes";
    _windows["records"] = window.open("/records", window.parent.name + "records", window_features);
}


function broadcastMessage(msg) {
    for (const [name, wndw] of Object.entries(_windows)) {
        console.log(`Post to ${name} = ${msg} by ${window.location.origin}`);
        wndw.postMessage(msg, window.location.origin);
    }
    if (window.opener) {
        window.opener.postMessage(msg, window.location.origin);
    }
}

function broadcastQuery(query, fquery) {
    const msg = {
        name: "set_query",
        q: query,
        fq: fquery
    }
    broadcastMessage(msg);
}

/**
 * Broadcast a message for the current spatial bounding box
 * @param bounds Leaflet bounds object
 */
function broadcastBoundingBox(bounds) {
    const msg = {
        name: "set_spatial_bounds",
        bounds: {
            south: bounds.getSouth(),
            north: bounds.getNorth(),
            west: bounds.getWest(),
            east: bounds.getEast()
        }
    }
    broadcastMessage(msg);
}
