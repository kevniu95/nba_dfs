from enum import Enum

class DfsSite(Enum):
    FANDUEL = "fanduel"
    DRAFTKINGS = "draftkings"
    # None means process both sites

POINTS_CONVERTER = {
    DfsSite.DRAFTKINGS :  {'PTS': 1, '3P': 0.5, 'TRB': 1.25, 'AST': 1.5, 'STL': 2, 'BLK': 2, 'TOV': -0.5, 'Double-Double': 1.5, 'Triple-Double': 3},
    DfsSite.FANDUEL :  {}
}
        