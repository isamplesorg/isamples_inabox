/*
Javascript support for spatial.html
 */

function queryParams() {
    return {
        q: DEFAULT_Q,
        fq: "",
        update(ev){
            this.q = ev.detail.q || this.q;
            this.fq = ev.detail.fq;
            updateHeatmapLayer(this.q, this.fq);
        }
    }
}

function displayMetadata() {
    return {
        total_docs: 0,
        match_docs: 0,
        show_docs: 0,
        last_modified: "",
        field_name: [],
        update(ev) {
            this.total_docs = ev.detail.total_docs || this.total_docs;
            this.match_docs = ev.detail.match_docs || this.match_docs;
            this.show_docs = ev.detail.show_docs || this.show_docs;
        }
    }
}


function notifyMetadataChanged() {
    let e = new CustomEvent("metadata-changed",{
        detail: {
            total_docs: collection_metadata.total_docs,
            match_docs: collection_metadata.match_docs,
            show_docs: collection_metadata.show_docs
        }
    });
    window.dispatchEvent(e);
}
