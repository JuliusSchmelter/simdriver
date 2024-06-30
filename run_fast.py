import subprocess
import os
from pathlib import Path
from glob import glob
from itertools import batched
import math
from shutil import copytree, rmtree

from openfast_toolbox.io import FASTInputFile, FASTOutputFile

DEFAULT_ELASTODYN_OUT = ["RotSpeed", "BldPitch1"]
DEFAULT_SERVODYN_OUT = ["GenPwr"]
RENAME = {
    "Time_[s]": "time",
    "Wind1VelX_[m/s]": "v0",
    "RotSpeed_[rpm]": "rot_speed",
    "BldPitch1_[deg]": "pitch",
    "GenPwr_[kW]": "power",
}


def run_fast(
    output_dir: str,
    model_dir: str,
    aerodyn: str,
    servodyn: str,
    elastodyn: str,
    beamdyn_blade: str | list[str] | None = None,
    subdyn: str | None = None,
    ext_ptfm: str | None = None,
    wind_files: str | list[str] | None = None,
    steady_wind_speed: float | list[float] | None = None,
    steady_power_law_exponent: float = 0.2,
    steady_reference_height: float | None = None,
    time_span: float = 660,
    time_step: float = 0.01,
    elastodyn_out: list[str] = [],
    servodyn_out: list[str] = [],
    custom_fast: str | None = None,
    fast_version: str = "3.5",
    max_processes: int = 32,
):
    # Path to resource directory.
    resources = Path(__file__).parent / "resources"

    # Path to FAST executable.
    if custom_fast is not None:
        fast_exe = f"{os.getcwd()}/{custom_fast}"
    else:
        fast_exe = resources / "OpenFAST.exe"

    # Create output directory if it does not exist.
    if not Path(output_dir).exists():
        Path(output_dir).mkdir(parents=True)

    # Load fast input file template.
    fst_file = FASTInputFile(resources / "fast_template.fst")

    # Apply scalar parameters.
    fst_file["TMax"] = time_span
    fst_file["DT"] = time_step

    ################################################################################################
    # Prepare inflow.
    # Load inflow file template.
    inflow_file = FASTInputFile(
        f"{resources}/inflow_template_{fast_version.replace(".", "_")}.dat"
    )

    # Set wind speed output at hub height.
    elastodyn_file = FASTInputFile(f"{model_dir}/{elastodyn}")
    hub_height = elastodyn_file["TowerHt"] + elastodyn_file["Twr2Shft"]
    inflow_file["WindVziList"] = hub_height

    # Collect inflow files.
    inflow_files = []

    # Steady wind speed.
    if steady_wind_speed is not None:
        # Make sure steady_wind_speed is a list.
        if not isinstance(steady_wind_speed, list):
            steady_wind_speed = [steady_wind_speed]

        # Set wind input to steady wind.
        inflow_file["WindType"] = 1

        # Configure steady wind inflow parameters.
        if steady_reference_height is None:
            steady_reference_height = hub_height

        inflow_file["RefHt"] = steady_reference_height
        inflow_file["PLexp"] = steady_power_law_exponent

        # Loop over wind speeds.
        for u in steady_wind_speed:
            inflow_file["HWindSpeed"] = u

            # Write inflow file.
            path = f"{os.getcwd()}/{output_dir}/U_{u}_TI_0.dat"
            inflow_file.write(path)
            inflow_files.append(path)

    # TurbSim wind input.
    elif steady_wind_speed is None:
        # Find TurbSim files.
        if not isinstance(wind_files, list):
            if wind_files is None:
                wind_files = output_dir

            bts_files = glob(f"{wind_files}/*.bts")
            if len(bts_files) == 0:
                raise ValueError("no wind input found.")
        else:
            bts_files = wind_files

        # Set wind input to TurbSim.
        inflow_file["WindType"] = 3

        # Loop over TurbSim files.
        for bts_file in bts_files:
            # Try both for compatibility with older versions.
            inflow_file["Filename"] = f'"{os.getcwd()}/{bts_file}"'
            inflow_file["Filename_BTS"] = f'"{os.getcwd()}/{bts_file}"'

            # Write inflow file.
            path = f"{os.getcwd()}/{output_dir}/{Path(bts_file).stem}.dat"
            inflow_file.write(path)
            inflow_files.append(path)

    else:
        raise ValueError("invalid combination of parameters.")

    ################################################################################################
    # Run FAST in parallel.
    # Iterate over inflow files in batches.
    counter = 1
    for batch in batched(inflow_files, max_processes):
        print(
            f"\nstarting batch {counter}/{math.ceil(len(inflow_files) / float(max_processes))} ...\n"
        )
        counter += 1

        # Iterate over batch and process it in parallel.
        outfiles = []
        temp_dirs = []
        tasks = []
        for inflow_file in batch:
            # Prepare temporary working directory.
            temp_dir = f"{os.getcwd()}/{output_dir}/temp_{Path(inflow_file).stem}"
            temp_dirs.append(temp_dir)

            # Clean up in case previous runs were aborted.
            try:
                rmtree(temp_dir)
            except Exception:
                pass

            copytree(model_dir, temp_dir)

            # Set FAST input file parameters.
            # ElastoDyn.
            elastodyn_path = f"{temp_dir}/{elastodyn}"
            fst_file["EDFile"] = f'"{elastodyn_path}"'
            elastodyn_file = FASTInputFile(elastodyn_path)
            elastodyn_file["OutList"] = [""] + DEFAULT_ELASTODYN_OUT + elastodyn_out
            elastodyn_file.write(elastodyn_path)

            # BeamDyn.
            if beamdyn_blade is not None:
                if not isinstance(beamdyn_blade, list):
                    beamdyn_blade = [beamdyn_blade] * 3

                for i, blade in enumerate(beamdyn_blade):
                    fst_file[f"BDBldFile({i+1})"] = f'"{temp_dir}/{blade}"'

            # AeroDyn.
            fst_file["AeroFile"] = f'"{temp_dir}/{aerodyn}"'

            # ServoDyn.
            servodyn_path = f"{temp_dir}/{servodyn}"
            fst_file["ServoFile"] = f'"{servodyn_path}"'
            servodyn_file = FASTInputFile(servodyn_path)
            servodyn_file["OutList"] = [""] + DEFAULT_SERVODYN_OUT + servodyn_out
            servodyn_file.write(servodyn_path)

            # SubDyn or ExtPtfm.
            if subdyn is not None:
                fst_file["CompSub"] = 1
                fst_file["SubFile"] = f'"{temp_dir}/{subdyn}"'

            if ext_ptfm is not None:
                fst_file["CompSub"] = 2
                fst_file["SubFile"] = f'"{temp_dir}/{ext_ptfm}"'

            # InflowWind.
            fst_file["InflowFile"] = f'"{inflow_file}"'

            # Write main fast input file.
            input_file = f"{output_dir}/{Path(inflow_file).stem}.fst"
            fst_file.write(input_file)

            # Run FAST.
            stdout = Path(input_file).with_suffix(".out")
            outfiles.append(stdout)
            tasks.append(
                subprocess.Popen(
                    [fast_exe, input_file],
                    stdout=open(stdout, "w"),
                    stderr=subprocess.STDOUT,
                )
            )

        # Wait for all tasks to finish.
        for task in tasks:
            task.wait()

        # Clean up temporary directories.
        for temp_dir in temp_dirs:
            rmtree(temp_dir)

        # Print stdout and stderr.
        for file in outfiles:
            print(f"########## {Path(file).stem} ##########\n")
            print(open(file, "r").read())

    ################################################################################################
    # Process output.
    for inflow_file in inflow_files:
        # Load FAST output file.
        output_file = FASTOutputFile(f"{output_dir}/{Path(inflow_file).stem}.outb")

        # Convert to parquet.
        output_file.toDataFrame().rename(columns=RENAME).to_parquet(
            f"{output_dir}/{Path(inflow_file).stem}.parquet"
        )

    # Print completion message.
    print("\nOpenFAST simulation completed.\n")
