import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
for p in (RAIZ / "dags", RAIZ / "databricks" / "lib", RAIZ / "scripts"):
    sys.path.insert(0, str(p))
FIXTURES = RAIZ / "tests" / "fixtures" / "chegada"
