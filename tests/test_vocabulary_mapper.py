from isamples_metadata.vocabularies import vocabulary_mapper


def test_controlled_vocabularies():
    assert vocabulary_mapper.MATERIAL_TYPE is not None
    assert vocabulary_mapper.SAMPLED_FEATURE is not None
    assert vocabulary_mapper.SPECIMEN_TYPE is not None
    past_human_activities = vocabulary_mapper.SAMPLED_FEATURE.term_for_key("pasthumanoccupationsite")
    assert past_human_activities is not None
    metadata_dict = past_human_activities.metadata_dict()
    assert metadata_dict.get("label") is not None
    assert metadata_dict.get("identifier") is not None
    past_human_activities_by_label = vocabulary_mapper.SAMPLED_FEATURE.term_for_label("Site of past human activities")
    assert past_human_activities_by_label is not None