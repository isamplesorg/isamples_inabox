from typing import Optional

import connegp
import fastapi


class Profile:
    uri: str
    token: str

    def __init__(self, uri: str, token: str):
        self.uri = uri
        self.token = token


ISAMPLES_PROFILE = Profile("https://w3id.org/isample/schema", "isamples")
SOURCE_PROFILE = Profile("https://w3id.org/isample/source_record", "source")
ALL_SUPPORTED_PROFILES = [ISAMPLES_PROFILE, SOURCE_PROFILE]


def content_profile_headers(profile: Profile) -> dict:
    return {
        "Content-Profile": profile.uri
    }

# taken and adapted from https://github.com/RDFLib/pyLDAPI
def get_profile_from_qsa(profiles_string: str = None) -> Optional[Profile]:
    """
    Reads _profile Query String Argument and returns the first Profile it finds
    Ref: https://www.w3.org/TR/dx-prof-conneg/#qsa-getresourcebyprofile
    :return: The profile to use, or None if not found
    :rtype: Profile
    """
    if profiles_string is not None:
        pqsa = connegp.ProfileQsaParser(profiles_string)
        if pqsa.valid:
            for profile in pqsa.profiles:
                if profile['profile'].startswith('<'):
                    # convert this valid URI/URN to a token
                    for supported_profile in ALL_SUPPORTED_PROFILES:
                        if supported_profile.uri == profile['profile'].strip('<>'):
                            return supported_profile
                else:
                    # convert this valid URI/URN to a token
                    for supported_profile in ALL_SUPPORTED_PROFILES:
                        if supported_profile.token == profile["profile"]:
                            return supported_profile

    return None