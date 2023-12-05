class TestQvidja:
    def test_request_qvidja_daily(self):
        from cams_radiation.cams_radiation import CDSRequest

        lat = 60.294264172757536
        lon = 22.390893932124552
        altitude = 12.28270435333252
        start_time = "2023-01-01"
        end_time = "2023-12-05"
        time_step = "1day"
        output_format = "csv"

        cds_request = CDSRequest(
            latitude=str(lat),
            longitude=str(lon),
            altitude=str(altitude),
            start_time=start_time,
            end_time=end_time,
            time_step=time_step,
            output_format=output_format,
        )

        df = cds_request.send_request()
