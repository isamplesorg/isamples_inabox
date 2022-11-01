import random

from isb_lib.identifiers.noidy.n2tminter import N2TMinter


def test_noidy():
    naan = random.randrange(100000)
    prefix = "fk4"
    num_identifiers = 10
    shoulder = f"{naan}/{prefix}"
    minter = N2TMinter(shoulder)
    _mint_identifiers_and_assert(minter, num_identifiers)


def _mint_identifiers_and_assert(minter, num_identifiers):
    identifiers = minter.mint(num_identifiers)
    returned_identifiers = 0
    for identifier in identifiers:
        returned_identifiers += 1
        assert len(identifier) > 0
    assert returned_identifiers == num_identifiers
