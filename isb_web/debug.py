import logging
import typing
from typing import Optional, Any

from fastapi import FastAPI, HTTPException
from isamples_metadata.SESARTransformer import SESARTransformer

from isamples_metadata.OpenContextTransformer import OpenContextTransformer

from isamples_metadata.GEOMETransformer import GEOMETransformer

from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer
from pydantic import BaseModel

from isamples_metadata.metadata_exceptions import MetadataException
from isb_web import isb_enums
from isb_web.isb_enums import ISBAuthority

debug_api = FastAPI()
# dao: Optional[SQLModelDAO] = None
DEBUG_PREFIX = "/debug"
logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("manage")


class DebugTransformParams(BaseModel):
    input_record: dict
    authority: ISBAuthority


@debug_api.post("/debug_transform")
def debug_transform(params: DebugTransformParams) -> Any:
    """Runs the transform for the specified input document
    Args:
        params: Class that contains the credentials and the data to post to datacite
    Return: The result of running the transformer
    """
    try:
        if params.authority == ISBAuthority.GEOME:
            return GEOMETransformer(params.input_record).transform()
        elif params.authority == ISBAuthority.OPENCONTEXT:
            return OpenContextTransformer(params.input_record).transform()
        elif params.authority == ISBAuthority.SESAR:
            return SESARTransformer(params.input_record).transform()
        elif params.authority == ISBAuthority.SMITHSONIAN:
            return SmithsonianTransformer(params.input_record).transform()
        else:
            raise HTTPException(500, "Specified authority is not an input we can transform")
    except MetadataException as e:
        raise HTTPException(415, str(e))