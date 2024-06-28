import subprocess
import os
from pathlib import Path
from glob import glob
from itertools import batched
import math
from shutil import copytree, rmtree

from openfast_toolbox.io import FASTInputFile, FASTOutputFile


# Deduplicate Pandas column names.
# https://stackoverflow.com/questions/40774787/renaming-columns-in-a-pandas-dataframe-with-duplicate-column-names
class renamer:
    def __init__(self):
        self.d = dict()

    def __call__(self, x):
        if x not in self.d:
            self.d[x] = 0
            return x
        else:
            self.d[x] += 1
            return "%s_%d" % (x, self.d[x])


def run_fast(
    output_dir: str,
    model_dir: str,
    elastodyn: str,
    aerodyn: str,
    servodyn: str,
    ext_ptfm: str,
    wind_files: str | list[str] | None = None,
    steady_wind_speed: float | list[float] | None = None,
    steady_power_law_exponent: float | None = None,
    steady_reference_height: float | None = None,
    time_span: float = 660,
    time_step: float = 0.01,
    custom_fast: str | None = None,
    fast_version: str = "3.5",
    max_processes: int = 32,
):
    # Path to resource directory.
    resources = Path(__file__).parent / "resources"

    # Create output directory if it does not exist.
    if not Path(output_dir).exists():
        Path(output_dir).mkdir(parents=True)

    # Load fast input file template.
    file = FASTInputFile(resources / "fast_template.fst")

    # Apply scalar parameters.
    file["TMax"] = time_span
    file["DT"] = time_step

    # Load inflow file template.
    inflow_file = FASTInputFile(
        f"{resources}/inflow_template_{fast_version.replace(".", "_")}.dat"
    )

    # Collect inflow files.
    inflow_files = []

    # Steady wind speed.
    if (
        steady_wind_speed is not None
        and steady_power_law_exponent is not None
        and steady_reference_height is not None
    ):
        # Make sure steady_wind_speed is a list.
        if not isinstance(steady_wind_speed, list):
            steady_wind_speed = [steady_wind_speed]

        # Set wind input to steady wind.
        inflow_file["WindType"] = 1

        # Configure steady wind inflow parameters.
        inflow_file["RefHt"] = steady_reference_height
        inflow_file["PLexp"] = steady_power_law_exponent

        # Loop over wind speeds.
        for u in steady_wind_speed:
            inflow_file["HWindSpeed"] = u

            # Write inflow file.
            path = f"{os.getcwd()}/{output_dir}/U_{u}.dat"
            inflow_file.write(path)
            inflow_files.append(path)

    # TurbSim wind input.
    elif (
        steady_wind_speed is None
        and steady_power_law_exponent is None
        and steady_reference_height is None
    ):
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

    # Path to FAST executable.
    if custom_fast is not None:
        fast_exe = f"{os.getcwd()}/{custom_fast}"
    else:
        fast_exe = resources / "OpenFAST.exe"

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
            file["EDFile"] = f'"{temp_dir}/{elastodyn}"'
            file["AeroFile"] = f'"{temp_dir}/{aerodyn}"'
            file["ServoFile"] = f'"{temp_dir}/{servodyn}"'
            file["SubFile"] = f'"{temp_dir}/{ext_ptfm}"'

            file["InflowFile"] = f'"{inflow_file}"'

            # Write main fast input file.
            input_file = f"{output_dir}/{Path(inflow_file).stem}.fst"
            file.write(input_file)

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

    # Convert output to parquet.
    for inflow_file in inflow_files:
        # Load FAST output file.
        output_file = FASTOutputFile(f"{output_dir}/{Path(inflow_file).stem}.outb")

        # Convert to parquet.
        output_file.toDataFrame().rename(columns=renamer()).to_parquet(
            f"{output_dir}/{Path(inflow_file).stem}.parquet"
        )

    # Print completion message.
    print("\nOpenFAST simulation completed.\n")
