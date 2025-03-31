import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    """
    create spark session
    """
    sparksession=get_spark_session("LOCAL")
    yield sparksession
    sparksession.stop()
