
"""Public orchestration entry point."""
from . import framework


def run(mode="full", con=None, db_path=None, recorded_at=None):
    from .config import SPEC
    from .store import Store
    own = con is None
    if con is None and db_path is not None:
        con = Store().connect(db_path)
    try:
        if SPEC.engine == "rpa":
            return framework.run_rpa(con=con, recorded_at=recorded_at)
        return framework.run(mode=mode, con=con, recorded_at=recorded_at)
    finally:
        if own and con is not None:
            con.close()
