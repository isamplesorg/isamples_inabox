# iSamples Relations

iSamples relations between records are stored as [nested Solr documents](https://solr.apache.org/guide/solr/latest/indexing-guide/indexing-nested-documents.html).  A large portion of the design is located in the [GitHub issue describing the feature](https://github.com/isamplesorg/isamples_inabox/issues/192).

## Overall design

We chose to store the nested documents on child records pointing back to their parents.  The most common type of relationship between samples are subsamples, so the nested relationship document lives on the subsample record, and it is pointing back to the parent record.  For example:

```
{
        "relation_target":"IGSN:ODP014BC3",
        "relation_type":"subsample",
        "id":"IGSN:ODP01QPHG_subsample_IGSN:ODP014BC3",
        "_nest_parent_":"IGSN:ODP01QPHG",
        "_version_":1740189557314289664
}
```

That document is a nested document on `IGSN:ODP01QPHG`, pointing back to its parent `IGSN:ODP014BC3`.  Currently, the id of the nested document is of the form `childid_relationtype_parentid`.  This may change in the future if it turns out to be wasteful for performance, but is currently helpful for debugging.

The way the relation documents are inserted is part of the solr reindexing process.  If the subsample document contains a key called `relations` -- it's expected that an array of relation documents like the one above will be inserted into the index.  Note that the relationship documents are actually separate documents in the same schema.  This is a bit strange in that the fields for the relation documents are only used by relation documents, but non-relation documents have those fields as well (and vice-versa).

## Querying

A powerful feature of the nested document design is that it is possible to search the entire graph hierarchy using solr's [graph traversal feature](https://solr.apache.org/guide/solr/latest/query-guide/graph-traversal.html).  Though it is a bit difficult to understand, it is incredibly powerful when used with [solr streaming expressions](https://solr.apache.org/guide/solr/latest/query-guide/streaming-expressions.html), as one can search for all parents/children that have a given property.  This is best understood in the context of an example.

The following streaming expression builds a graph of all of the subsamples (and subsequent subsamples) of a sample with the label "PI55":

```
nodes(isb_core_records, 
    search(isb_core_records, q="{!child of="*:* -_nest_path_:*"}label:PI55", fl="id,relation_target,relation_type,_nest_parent_"), 
    walk="_nest_parent_-> relation_target",
    gather="_nest_parent_",
    scatter="branches,leaves"
)
```

This query is a generalized tree-building query and can help explain what is going on:

```
fetch(isb_core_records,
  nodes(isb_core_records, 
    search(isb_core_records, q="relation_type:subsample", fl="*,relation_target,_nest_parent_"), 
    walk="relation_target->_nest_parent_",
    gather="relation_target",
    scatter="branches,leaves"
  ),
  fl="id,label,[child]",
  on="node=id"
)
```

This first searches for all the subsample relationship documents (stored on the child).  Then, it walks from the nested document to the containing child (`_nest_parent_`) and adds the nest parent.  Then, this bit (`relation_target->_nest_parent_`) traverses the link from the relationship document to the target of the relationship document (the parent document in a subsample type of relationship).  The `gather="relation_target"` tells solr to continue walking that node until it reaches the end of the line.  All that we've described there is happening in the inner `nodes()` streaming expression.  The outer `fetch()` streaming expression is joining the intermediate graph edges with the actual documents in the index (the `on="node=id"` does that bit).  Finally, one other thing to mention, in the `fetch`, one of the fields is labeled `[child]` -- this specifies to fetch all fields of any nested documents encountered in the graph traversal.

## Relationship types

As of now, all relationships are marked as `subsample` relationship types.  It is expected that the number of types will grow as iSamples grows.