import typing
import click
import click_config_file
import isb_lib.core
import csv
from isb_web.sqlmodel_database import SQLModelDAO

excluded_keys = ["id", "bcid", "uri", "@id"]


def process_dictionary(
    base_key: typing.Optional[str],
    field_names: set[str],
    in_dict: typing.Dict,
    out_dict: typing.Dict,
):
    """Process all the keys in a dictionary while populating field_names and out_dict, and recursing through embedded
     dicts"""
    for key in in_dict:
        out_dict_key = key if base_key is None else f"{base_key}_{key}"
        field_names.add(out_dict_key)
        if key not in excluded_keys:
            value = in_dict[key]
            if type(value) is dict:
                process_dictionary(out_dict_key, field_names, value, out_dict)
            elif type(value) is str:
                out_dict[out_dict_key] = value
            elif type(value) is list:
                total_value = ""
                for subvalue in value:
                    if type(subvalue) is dict:
                        process_dictionary(
                            out_dict_key, field_names, subvalue, out_dict
                        )
                    else:
                        total_value = total_value + str(subvalue)
                if len(total_value) > 0:
                    # if we had any non-embedded values, write to the out dict, otherwise prefer embedded
                    out_dict[out_dict_key] = total_value
            else:
                out_dict[out_dict_key] = str(value)


# Don't assume every record has all the fields -- check each one and pick the one with the most
def max_fieldnames(
    current_fieldnames: typing.Optional[typing.List], new_dict: typing.Dict
):
    new_fieldnames = []
    for key in new_dict:
        new_fieldnames.append(key)
    if current_fieldnames is None or len(new_fieldnames) > len(current_fieldnames):
        return new_fieldnames
    else:
        return current_fieldnames


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnostics on 9999"
)
@click.option(
    "-a",
    "--authority",
    default="SMITHSONIAN",
    help="Which authority to use when selecting and dumping the resolved_content",
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate, authority):
    isb_lib.core.things_main(ctx, db_url, None, verbosity, heart_rate)
    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    sql = f"""select resolved_content from thing tablesample system(0.5) where
    resolved_status=200 and authority_id='{authority}'"""
    rs = session.execute(sql)
    filename = f"{authority}.txt"
    result_set_dicts = []
    header_fieldnames = set()
    for row in rs:
        resolved_content = row._asdict()["resolved_content"]
        processed_resolved_content = {}
        process_dictionary(
            None, header_fieldnames, resolved_content, processed_resolved_content
        )
        result_set_dicts.append(processed_resolved_content)

    with open(filename, "w", newline="") as file:
        writer = csv.DictWriter(
            file, delimiter="#", quoting=csv.QUOTE_ALL, fieldnames=list(header_fieldnames)
        )
        writer.writeheader()
        for result_set_dict in result_set_dicts:
            writer.writerow(result_set_dict)


"""
Creates a text dump of the source collection file, one line of text per record
"""
if __name__ == "__main__":
    main()
