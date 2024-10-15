from pathlib import Path
import os
import contextlib
import pandas as pd
import numpy as np
from shutil import rmtree
import polars as pl
from polars import col
from weio.fast_wind_file import FASTWndFile

from . import run_fast


def initial_state(
    input_file: str,
    openfast_time_step: float,
    fast_version: str,
    custom_fast: str | None,
    verbose: bool,
    initial_state_output: str | None = None,
    min_speed: float = 3,
    max_speed: float = 25,
    step_size: float = 2,
    startup_time: float = 300,
    rise_time: float = 30,
    time_at_speed: float = 120,
    analyzed_fraction: float = 0.25,
    wind_time_step: float = 0.1,
    retain_temp_files: bool = False,
):
    """
    Run OpenFAST to find initial turbine state for different wind speeds.

    Args:
        input_file: Path to the OpenFAST input file.
        time_step: OpenFAST simulation time step.
        fast_version: Version of custom OpenFAST executable.
        custom_fast: Relative path to custom OpenFAST executable.
        verbose: Print stdout and stderr of OpenFAST processes.
        initial_state_output: Custom path to the output initial state file, optional.
        min_speed: Minimum wind speed in m/s.
        max_speed: Maximum wind speed in m/s.
        step_size: Wind speed step size in m/s.
        rise_time: Time for linear ramp between wind speed steps, in seconds.
        time_at_speed: Time to stay at the wind speed step in seconds.
        analyzed_fraction: Fraction of the time at speed to analyze for the initial state.
        wind_time_step: Wind speed time step in seconds.
    """
    # Path to model directory.
    model_dir = Path(input_file).parent

    wind_steps = np.arange(min_speed, max_speed + step_size, step_size)
    time = [0]
    speed = [0]
    windows = []
    startup = True
    for wind_step in wind_steps:
        v_0 = speed[-1]
        v = speed[-1]
        t = time[-1]

        # Startup.
        if startup:
            windows.append(
                {
                    "v0": wind_step,
                    "start": startup_time + time_at_speed * (1 - analyzed_fraction),
                    "end": startup_time + time_at_speed,
                }
            )

            for _ in range(round(startup_time / wind_time_step)):
                t += wind_time_step
                time.append(t)
                speed.append(wind_step)

            startup = False

        # Linear rise.
        else:
            windows.append(
                {
                    "v0": wind_step,
                    "start": t + rise_time + time_at_speed * (1 - analyzed_fraction),
                    "end": t + rise_time + time_at_speed,
                }
            )

            for _ in range(round(rise_time / wind_time_step)):
                t += wind_time_step
                v += ((wind_step - v_0) / rise_time) * wind_time_step
                time.append(t)
                speed.append(v)

        # Constant speed.
        for _ in range(round(time_at_speed / wind_time_step)):
            t += wind_time_step
            time.append(t)
            speed.append(wind_step)

    # Write wind file.
    wnd_file = FASTWndFile()
    wnd_file.data = pd.DataFrame(
        {
            "Time_[s]": time,
            "WindSpeed_[m/s]": speed,
            "WindDir_[deg]": 0,
            "VertSpeed_[m/s]": 0,
            "HorizShear_[-]": 0,
            "VertShear_[-]": 0.17,
            "LinVShear_[-]": 0,
            "GustSpeed_[m/s]": 0,
        }
    )

    # Clean up in case previous runs were aborted.
    try:
        rmtree("simdriver_temp")
    except Exception:
        pass

    os.mkdir("simdriver_temp")

    wnd_file_path = "simdriver_temp/step_wind.hh"

    # Suppress unnecessary error messages.
    with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
        wnd_file.write(wnd_file_path)

    # Run OpenFAST.
    run_fast.run_fast(
        output_dir="simdriver_temp",
        input_file=input_file,
        wind_files=[wnd_file_path],
        time_span=time[-1],
        time_step=openfast_time_step,
        custom_fast=custom_fast,
        fast_version=fast_version,
        verbose=verbose,
        initialize_turbine_state=False,
        elastodyn_out=[
            "OoPDefl1",
            "IPDefl1",
            "TTDspFA",
            "TTDspSS",
        ],
    )

    # Analyze results.
    res = pl.read_parquet("simdriver_temp/step_wind.parquet")

    initial_states = []
    for window in windows:
        window_res = res.filter(
            col("time") >= window["start"], col("time") < window["end"]
        )

        initial_states.append(
            {
                "v0": window["v0"],
                "pitch": window_res["pitch"].mean(),
                "rot_speed": window_res["rot_speed"].mean(),
                "OoPDefl": window_res["OoPDefl1_[m]"].mean(),
                "IPDefl": window_res["IPDefl1_[m]"].mean(),
                "TTDspFA": window_res["TTDspFA_[m]"].mean(),
                "TTDspSS": window_res["TTDspSS_[m]"].mean(),
            }
        )

    initial_states = pl.DataFrame(initial_states)

    # Write initial states to file.
    if initial_state_output is None:
        initial_state_output = model_dir / "simdriver_initial_state.csv"

    initial_states.write_csv(initial_state_output)

    # Clean up.
    if not retain_temp_files:
        # Ugly hack, I don't know why this is necessary.
        for _ in range(10):
            try:
                rmtree("simdriver_temp", ignore_errors=True)
                Path("simdriver_temp").rmdir()
            except Exception:
                pass

    return initial_states
