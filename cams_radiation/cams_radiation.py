#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author:
    Olli Nevalainen (olli.nevalainen@fmi.fi), Finnish Meteorological Institute)
"""
import pandas as pd
import datetime
import io
import requests
import xarray as xr

import cdsapi
from pydantic import BaseModel, model_validator
from typing import Literal, Optional, Union
try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
    from enum import Enum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass

PAR_FRACTION = 0.5
J_TO_UMOL = 4.6

CAMS_SOLAR_RADIATION_TIMESERIES = "cams-solar-radiation-timeseries"
CAMS_TIME_COL = "Observation_period"


class Units(StrEnum):
    GLOBAL_IRRADIATION = "global_irradiation(Whm-2)"
    SHORTWAVE_RADIATION = "shorwave_radiation(Wm-2)"
    PPFD = "PPFD(umolm-2s-1)"
    PAR = "PAR(MJm-2)"


class CDSRequest(BaseModel):
    """CDS, Climate Data Store"""

    latitude: str
    longitude: str
    altitude: str
    start_time: str
    end_time: str
    time_step: Literal["1minute", "15minute", "1hour", "1day", "1month"]
    output_format: Literal["csv", "netcdf", "csv_expert"]
    cdsapi_kwargs: Optional[dict] = None

    @model_validator(mode="after")
    def check_csv_expert_timestep(self) -> "CDSRequest":
        if self.output_format == "csv_expert" and not self.time_step == "1minute":
            raise ValueError("csv_expert mode only available for 1minute timestep")
        return self

    def send_request(
        self, output_file: Optional[str] = None
    ) -> Union[pd.DataFrame, xr.Dataset, None]:
        if self.cdsapi_kwargs:
            c = cdsapi.Client(**self.cdsapi_kwargs)
        else:
            c = cdsapi.Client()

        params = {
            "location": {
                "latitude": self.latitude,
                "longitude": self.longitude,
            },
            "altitude": self.altitude,
            "date": f"{self.start_time}/{self.end_time}",
            "sky_type": "observed_cloud",
            "time_step": self.time_step,
            "time_reference": "universal_time",
            "format": self.output_format,
        }
        result = c.retrieve(CAMS_SOLAR_RADIATION_TIMESERIES, params, output_file)

        if not output_file:
            if self.output_format in ["csv", "csv_expert"]:
                df = _read_csv_url_to_dataframe(result.location)
                return df
            if self.output_format == "netcdf":
                ds = _read_netcdf_url_to_dataset(result.location)
                return ds


def _read_csv_url_to_dataframe(url: str) -> pd.DataFrame:
    response = requests.get(url)
    data = io.StringIO(response.text.split("#")[-1])
    df = pd.read_csv(data, delimiter=";")
    # Strip empty spaces from headers
    df.columns = df.columns.str.lstrip().str.rstrip().str.replace(" ", "_")
    return df


def _read_netcdf_url_to_dataset(url: str) -> pd.DataFrame:
    response = requests.get(url)
    ds = xr.open_dataset(response.content)
    return ds


def observation_period_to_index(df: pd.DataFrame, new_index_name: Optional[str] = None):
    df[CAMS_TIME_COL] = pd.to_datetime(
        df.apply(lambda x: x[CAMS_TIME_COL].split("/")[0], axis=1)
    )
    df.set_index(CAMS_TIME_COL, drop=True, inplace=True)
    if new_index_name:
        df.index.name = new_index_name
    return df


def calculate_PAR(df_irradiation: pd.DataFrame):
    time_delta = df_irradiation.index[1] - df_irradiation.index[0]
    s_in_1h = 3600
    if time_delta.days != 0:
        time_delta_seconds = s_in_1h * 24 * time_delta.days
    else:
        time_delta_seconds = time_delta.seconds

    df_par = pd.DataFrame(index=df_irradiation.index)

    # GHI in units Wh/m2
    df_par[Units.GLOBAL_IRRADIATION] = df_irradiation["GHI"].copy(deep=True)

    df_par[Units.SHORTWAVE_RADIATION] = (
        df_par[Units.GLOBAL_IRRADIATION] * s_in_1h / time_delta_seconds
    )  # s_in_1h converts units to Jm-2 (Wsm-2) and dividing by obs period to Wm-2
    df_par[Units.PAR] = (
        df_par[Units.GLOBAL_IRRADIATION] * s_in_1h * PAR_FRACTION * 1e-6
    )  # after this units are MJm-2 (=MW-m2) (PAR), summation over obs period (time_delta.seconds)

    df_par[Units.PPFD] = df_par[Units.SHORTWAVE_RADIATION] * PAR_FRACTION * J_TO_UMOL
    # units umolm-2s-1

    return df_par


def calculate_aggregated_par(df_irradiation: pd.DataFrame, aggregation_level: str):
    time_delta = df_irradiation.index[1] - df_irradiation.index[0]

    assert_error_message = f"""Aggregation level {aggregation_level} not possible with current data time step: {time_delta} s"""

    s_in_min = 60
    s_in_30min = 1800
    s_in_1h = 3600
    s_in_day = 86400

    if aggregation_level == "daily":
        if time_delta.days == 0:
            assert time_delta.seconds < s_in_day, assert_error_message
        aggregator_param = "1D"
        par_units = "PAR [MJ/m2/d]"
    elif aggregation_level == "hourly":
        assert time_delta.seconds < s_in_1h, assert_error_message
        aggregator_param = "1H"
        par_units = "PAR [MJ/m2/h]"
    elif aggregation_level == "30min":
        assert time_delta.seconds < s_in_30min, assert_error_message
        aggregator_param = "30Min"
        par_units = "PAR [MJ/m2/30min]"
    else:
        raise NotImplementedError(
            f"Aggregation level {aggregation_level} not implemented!"
        )

    df_aggr = calculate_PAR(df_irradiation)

    aggregators = {
        Units.GLOBAL_IRRADIATION: "sum",
        Units.SHORTWAVE_RADIATION: "mean",
        Units.PAR: "sum",
        Units.PPFD: "mean",
    }
    df_aggr = df_aggr.resample(aggregator_param).agg(aggregators)
    # TODO: Verify this output!
    return df_aggr


def datetime_from_date_and_hour(date, hour):
    return datetime.datetime.combine(date, datetime.time(hour))
