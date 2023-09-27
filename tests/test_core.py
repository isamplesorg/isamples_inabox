import click.core
import pytest
import isb_lib.core
import json
import requests

from isb_lib.core import things_main

TEST_LIVE_SERVER = 0

iterator_testcases = [
    (1, 1),
    (55, 55),
    (999, 999),
    (1000, 1000),
    (1001, 1000),
    (2020, 1000),
]


@pytest.mark.parametrize("max_entries,expected_outcome", iterator_testcases)
def test_IdentifierIterator(max_entries, expected_outcome):
    itr = isb_lib.core.IdentifierIterator(max_entries=max_entries)
    cnt = 0
    for e in itr:
        cnt += 1
    assert cnt == expected_outcome


def _try_to_add_solr_doc(core_doc_str):
    core_doc = json.loads(core_doc_str)
    solr_dict = isb_lib.core._coreRecordAsSolrDoc(core_doc)
    if TEST_LIVE_SERVER:
        isb_lib.core.solrAddRecords(requests.session(), [solr_dict], url="http://localhost:8983/api/collections/isb_core_records/")
    return solr_dict


def test_coreRecordAsSolrDoc():
    core_doc_str = """
{
	"$schema": "iSamplesSchemaCore1.0.json",
	"@id": "https://data.isamples.org/digitalsample/igsn/EOI00002H",
	"label": "J730-GTHFS-16",
	"sample_identifier": "IGSN:EOI00002H",
	"description": "",
	"has_context_category": [
		{
			"label": "subsurfacefluidreservoir"
		}
	],
	"has_context_category_confidence": [
		1.0
	],
	"has_material_category": [
		{
			"label": "gas"
		}
	],
	"has_material_category_confidence": null,
	"has_specimen_category": [
		{
			"label": "othersolidobject"
		}
	],
	"has_specimen_category_confidence": [
		1.0
	],
	"informal_classification": [
		""
	],
	"keywords": [
		"Individual Sample"
	],
	"produced_by": {
		"@id": "",
		"label": "Sampler:Fluid:GTHFS",
		"description": "cruiseFieldPrgrm:TN300. launchPlatformName:Jason II. Sampler:Fluid:GTHFS. HFS gastight. Red-center-9. T=250C. launch type:ROV, navigation type:USBL",
		"has_feature_of_interest": "volcano",
		"responsibility": [
			"Evans_Leigh,,Collector",
			"Andra Bobbitt,,Sample Owner"
		],
		"result_time": "2013-09-14 01:30:00",
		"sampling_site": {
			"description": "Trevi:Jason Tmax=257.9 C. In the direct flow at this small anhydrite mound (anhydrite knocked over).",
			"label": "",
			"sample_location": {
				"elevation": "-1520.0 m",
				"latitude": 45.9463,
				"longitude": -129.9837
			},
			"place_name": [
				"Axial Seamount"
			]
		}
	},
	"registrant": {
		"name": "Andra Bobbitt"
	},
	"sampling_purpose": "",
	"curation": {
		"label": "",
		"description": "",
		"access_constraints": "",
		"curation_location": "",
		"responsibility": [

		]
	},
	"related_resource": [

	],
	"authorized_by": [

	],
	"complies_with": [

	],
	"producedBy_samplingSite_location_h3_0": "8029fffffffffff",
	"producedBy_samplingSite_location_h3_1": "8128fffffffffff",
	"producedBy_samplingSite_location_h3_2": "8228effffffffff",
	"producedBy_samplingSite_location_h3_3": "8328edfffffffff",
	"producedBy_samplingSite_location_h3_4": "8428ed7ffffffff",
	"producedBy_samplingSite_location_h3_5": "8528ed7bfffffff",
	"producedBy_samplingSite_location_h3_6": "8628ed797ffffff",
	"producedBy_samplingSite_location_h3_7": "8728ed791ffffff",
	"producedBy_samplingSite_location_h3_8": "8828ed791bfffff",
	"producedBy_samplingSite_location_h3_9": "8928ed791bbffff",
	"producedBy_samplingSite_location_h3_10": "8a28ed791baffff",
	"producedBy_samplingSite_location_h3_11": "8b28ed791a36fff",
	"producedBy_samplingSite_location_h3_12": "8c28ed791ba97ff",
	"producedBy_samplingSite_location_h3_13": "8d28ed791ba96ff",
	"producedBy_samplingSite_location_h3_14": "8e28ed791ba96ef"
}
    """
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "producedBy_samplingSite_location_ll" in solr_dict
    assert "producedBy_samplingSite_location_h3_0" in solr_dict
    assert "producedBy_samplingSite_location_h3_15" in solr_dict


def test_coreRecordAsSolrDoc2():
    core_doc_str = """
{
	"$schema": "iSamplesSchemaCore1.0.json",
	"@id": "metadata/28722/k2ks6xw6t",
	"label": "Object Object 68",
	"sample_identifier": "ark:/28722/k2ks6xw6t",
	"description": "'updated': 2023-07-18T02:30:12Z | 'Has type': color patches (military patches)",
	"has_context_category": [
		{
			"label": "Site of past human activities",
			"identifier": "https://w3id.org/isample/vocabulary/sampledfeature/0.9/pasthumanoccupationsite"
		}
	],
	"has_context_category_confidence": [
		1.0
	],
	"has_material_category": [
		{
			"label": "mat:rock"
		},
		{
			"label": "ocmat:ceramicclay"
		},
		{
			"label": "mat:anthropogenicmetal"
		}
	],
	"has_material_category_confidence": [
		0.7636867165565491,
		0.08359246701002121,
		0.07399434596300125
	],
	"has_specimen_category": [
		{
			"label": "physicalspecimen"
		}
	],
	"has_specimen_category_confidence": [
		1.0
	],
	"informal_classification": [

	],
	"keywords": [
		{
			"keyword": "Space archaeology"
		},
		{
			"keyword": "Space stations"
		},
		{
			"keyword": "International Space Station"
		},
		{
			"keyword": "Religion"
		}
	],
	"produced_by": {
		"@id": "",
		"label": "Archaeology of the International Space Station",
		"description": "https://opencontext.org/projects/e682f907-6e4a-44cc-8a5f-3e2c73001673",
		"has_feature_of_interest": "",
		"responsibility": [
			{
				"role": "creator",
				"name": "Justin Walsh"
			},
			{
				"role": "creator",
				"name": "Alice Gorman"
			},
			{
				"role": "creator",
				"name": "Wendy Salmond"
			}
		],
		"result_time": "2021-01-27T02:47:12Z",
		"sampling_site": {
			"description": "",
			"label": "Off World/International Space Station/Zvezda Service Module",
			"sample_location": {
				"elevation": "",
				"latitude": null,
				"longitude": null
			},
			"place_name": [
				"Off World",
				"International Space Station",
				"Zvezda Service Module"
			]
		}
	},
	"registrant": {
		"name": ""
	},
	"sampling_purpose": "",
	"curation": {
		"label": "",
		"description": "",
		"access_constraints": "",
		"curation_location": "",
		"responsibility": [

		]
	},
	"related_resource": [

	],
	"authorized_by": [

	],
	"complies_with": [

	],
	"producedBy_samplingSite_location_h3_0": null,
	"producedBy_samplingSite_location_h3_1": null,
	"producedBy_samplingSite_location_h3_2": null,
	"producedBy_samplingSite_location_h3_3": null,
	"producedBy_samplingSite_location_h3_4": null,
	"producedBy_samplingSite_location_h3_5": null,
	"producedBy_samplingSite_location_h3_6": null,
	"producedBy_samplingSite_location_h3_7": null,
	"producedBy_samplingSite_location_h3_8": null,
	"producedBy_samplingSite_location_h3_9": null,
	"producedBy_samplingSite_location_h3_10": null,
	"producedBy_samplingSite_location_h3_11": null,
	"producedBy_samplingSite_location_h3_12": null,
	"producedBy_samplingSite_location_h3_13": null,
	"producedBy_samplingSite_location_h3_14": null
}
    """
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "producedBy_samplingSite_location_latlon" not in solr_dict


def test_date_year_only():
    date_str = "1985"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.day == 1
    assert datetime.month == 1
    assert datetime.year == 1985


def test_date_year_month_day():
    date_str = "1947-08-06"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.day == 6
    assert datetime.month == 8
    assert datetime.year == 1947


def test_date_year_month():
    date_str = "2020-07"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.month == 7
    assert datetime.year == 2020
    # default to the first of the month since it wasn't in the original
    assert datetime.day == 1


def test_date_with_time():
    date_str = "2019-12-08 15:54:00"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.year == 2019
    assert datetime.month == 12
    assert datetime.day == 8
    assert datetime.hour == 15
    assert datetime.minute == 54
    assert datetime.second == 0
    assert datetime.tzinfo.zone == 'UTC'


def test_isamples_date():
    datetime = isb_lib.core.parsed_datetime_from_isamples_format("2020-07-16T11:25:16Z")
    assert datetime is not None
    assert datetime.year == 2020
    assert datetime.month == 7
    assert datetime.day == 16
    assert datetime.hour == 11
    assert datetime.minute == 25
    assert datetime.second == 16


def test_isamples_date_with_ms():
    datetime = isb_lib.core.parsed_datetime_from_isamples_format("2020-07-16T11:25:16.123Z")
    assert datetime is not None
    assert datetime.year == 2020
    assert datetime.month == 7
    assert datetime.day == 16
    assert datetime.hour == 11
    assert datetime.minute == 25
    assert datetime.second == 16


def test_things_main():
    things_main(click.core.Context(click.core.Command("test")), None, None)
