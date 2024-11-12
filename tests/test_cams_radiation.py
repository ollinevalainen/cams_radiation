import os
from cams_radiation.cams_radiation import CDSRequest


class TestQvidja:
    def test_request_qvidja_daily(self):

        lat = 60.294264172757536
        lon = 22.390893932124552
        altitude = 12.28270435333252
        start_time = "2023-01-01"
        end_time = "2023-12-05"
        time_step = "1day"
        output_format = "csv"
        cdsapi_kwargs = check_environ_cdsapi_configuration()
        cds_request = CDSRequest(
            latitude=str(lat),
            longitude=str(lon),
            altitude=str(altitude),
            start_time=start_time,
            end_time=end_time,
            time_step=time_step,
            output_format=output_format,
            cdsapi_kwargs=cdsapi_kwargs,
        )

        df = cds_request.send_request()


def check_environ_cdsapi_configuration():
    """Function for checking if CDSAPI configuration envionment variables are set. Used in tests."""
    # Check if CDSAPI configuration envionment variables are set
    cdsapi_key = os.environ.get("CDSAPI_KEY_PYTEST")
    cdsapi_url = os.environ.get("CDSAPI_URL_PYTEST")
    if cdsapi_key is not None and cdsapi_url is not None:
        cdsapi_kwargs = {"key": cdsapi_key, "url": cdsapi_url}
    else:
        cdsapi_kwargs = None
    return cdsapi_kwargs
