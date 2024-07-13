import subprocess
import os
from pathlib import Path
from glob import glob
from itertools import batched
import math
from shutil import copytree, rmtree

from openfast_toolbox.io import FASTInputFile, FASTOutputFile

DEFAULT_ELASTODYN_OUT = [
    "RotSpeed",
    "BldPitch1",
    "RotTorq",
    "RootMEdg1",
    "RootMFlp1",
    "RootMEdg2",
    "RootMFlp2",
    "RootMEdg3",
    "RootMFlp3",
    "TwrBsMxt",
    "TwrBsMyt",
]
DEFAULT_SERVODYN_OUT = ["GenPwr"]
RENAME = {
    "Time_[s]": "time",
    "Wind1VelX_[m/s]": "v0",
    "RotSpeed_[rpm]": "rot_speed",
    "RotTorq_[kN-m]": "rot_torque",
    "GenPwr_[kW]": "gen_power",
    "BldPitch1_[deg]": "pitch",
    "RootMEdg1_[kN-m]": "M_b1_e",
    "RootMFlp1_[kN-m]": "M_b1_f",
    "RootMEdg2_[kN-m]": "M_b2_e",
    "RootMFlp2_[kN-m]": "M_b2_f",
    "RootMEdg3_[kN-m]": "M_b3_e",
    "RootMFlp3_[kN-m]": "M_b3_f",
    "TwrBsMyt_[kN-m]": "M_tower_fa",
    "TwrBsMxt_[kN-m]": "M_tower_ss",
}


def run_fast(
    output_dir: str,
    model_dir: str,
    input_file: str,
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

    # Load input file template.
    version_id = fast_version.replace(".", "_")
    try:
        inflow_file = FASTInputFile(f"{resources}/inflow_template_{version_id}.dat")
    except Exception:
        raise ValueError("Error: OpenFAST version not supported.")

    ################################################################################################
    # Prepare inflow.
    # Set wind speed output at hub height.
    fst_file_template = FASTInputFile(f"{os.getcwd()}/{model_dir}/{input_file}")
    elastodyn_file = FASTInputFile(
        f"{os.getcwd()}/{model_dir}/{fst_file_template["EDFile"].strip('"')}"
    )
    hub_height = elastodyn_file["TowerHt"] + elastodyn_file["Twr2Shft"]
    inflow_file["WindVziList"] = hub_height

    # Collect inflow files.
    inflow_files = []

    # Steady wind speed.
    if steady_wind_speed is not None:
        # Make sure steady_wind_speed is a list.
        if isinstance(steady_wind_speed, int) or isinstance(steady_wind_speed, float):
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

    # Turbulent wind input.
    elif steady_wind_speed is None:
        # Find input files.
        if not isinstance(wind_files, list):
            if wind_files is None:
                wind_files = output_dir

            turb_files = glob(f"{wind_files}/*.bts") + glob(f"{wind_files}/*.wnd")
            if len(turb_files) == 0:
                raise ValueError("no wind input found.")
        else:
            turb_files = wind_files

        # Loop over TurbSim files.
        for turb_file in turb_files:
            if turb_file.endswith("bts"):
                # Set wind input to TurbSim.
                inflow_file["WindType"] = 3

                # Try both for compatibility with older versions.
                inflow_file["Filename"] = f'"{os.getcwd()}/{turb_file}"'
                inflow_file["Filename_BTS"] = f'"{os.getcwd()}/{turb_file}"'

            elif turb_file.endswith("wnd"):
                # Set wind input to TurbSim.
                inflow_file["WindType"] = 4

                inflow_file["FilenameRoot"] = (
                    f'"{os.getcwd()}/{turb_file.removesuffix(".wnd")}"'
                )

            else:
                raise ValueError("Unknown wind file. Use '.bts' or '.wnd'.")

            # Write inflow file.
            path = f"{os.getcwd()}/{output_dir}/{Path(turb_file).stem}.dat"
            inflow_file.write(path)
            inflow_files.append(path)

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

            # Prepare FAST input file.
            fst_file = FASTInputFile(f"{os.getcwd()}/{model_dir}/{input_file}")

            fst_file["InflowFile"] = f'"{inflow_file}"'

            fst_file["TMax"] = time_span
            fst_file["DT"] = time_step
            fst_file["OutFileFmt"] = 2
            fst_file["SumPrint"] = True

            dat_files = [
                "EDFile",
                "BDBldFile(1)",
                "BDBldFile(2)",
                "BDBldFile(3)",
                "AeroFile",
                "ServoFile",
                "HydroFile",
                "SubFile",
                "MooringFile",
                "IceFile",
                "SWELidarFile",
            ]
            for dat_file in dat_files:
                try:
                    fst_file[dat_file] = f'"{temp_dir}/{fst_file[dat_file].strip('"')}"'
                except Exception:
                    pass

            # Set output parameters.
            # ElastoDyn.
            elastodyn_file = FASTInputFile(fst_file["EDFile"].strip('"'))
            elastodyn_file["OutList"] = [""] + DEFAULT_ELASTODYN_OUT + elastodyn_out
            elastodyn_file.write(fst_file["EDFile"].strip('"'))

            # ServoDyn.
            servodyn_file = FASTInputFile(fst_file["ServoFile"].strip('"'))
            servodyn_file["OutList"] = [""] + DEFAULT_SERVODYN_OUT + servodyn_out
            servodyn_file.write(fst_file["ServoFile"].strip('"'))

            # Write input file.
            fst_file_path = f"{output_dir}/{Path(inflow_file).stem}.fst"
            fst_file.write(fst_file_path)

            # Run FAST.
            stdout = Path(fst_file_path).with_suffix(".out")
            outfiles.append(stdout)
            tasks.append(
                subprocess.Popen(
                    [fast_exe, fst_file_path],
                    stdout=open(stdout, "w"),
                    stderr=subprocess.STDOUT,
                )
            )

        # Wait for all tasks to finish.
        error = False
        for task in tasks:
            return_code = task.wait()
            if return_code != 0:
                error = True

        # Print stdout and stderr.
        for file in outfiles:
            print(f"########## {Path(file).stem} ##########\n")
            print(open(file, "r").read())

        # Exit due to error or clean up temporary directories.
        if error:
            raise Exception("OpenFAST Error")
        else:
            for temp_dir in temp_dirs:
                # Ugly hack, I don't know why this is necessary.
                for _ in range(10):
                    try:
                        rmtree(temp_dir, ignore_errors=True)
                        Path(temp_dir).rmdir()
                    except Exception:
                        pass

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
