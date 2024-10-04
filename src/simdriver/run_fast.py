import subprocess
import os
from pathlib import Path
from glob import glob
from itertools import batched
import math
from shutil import copytree, rmtree

from weio import FASTInputFile, FASTOutputFile

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
    max_processes: int = 20,
    verbose: bool = False,
    initialize_turbine_state: bool = True,
    initialization_options: dict = {},
):
    """
    Run OpenFAST in parallel for multiple wind conditions.

    Args:
        output_dir: Relative path to output directory.
        input_file: Path to OpenFAST input file (.fst).
        wind_files: Relative path to directory with TurbSim files (.bts, .wnd)
                    or list of relative paths to TurbSim files.
        steady_wind_speed: Steady wind speed or list of steady wind speeds to be
                    used instead of TurbSim input.
        steady_power_law_exponent: Power law exponent for steady wind input.
        steady_reference_height: Reference height for steady wind input.
        time_span: Simulation time span.
        time_step: Simulation time step.
        elastodyn_out: Additional ElastoDyn output parameters.
        servodyn_out: Additional ServoDyn output parameters.
        custom_fast: Relative path to custom OpenFAST executable.
        fast_version: Version of custom OpenFAST executable.
        max_processes: Maximum number of parallel processes.
        verbose: Print stdout and stderr of OpenFAST processes.
        initialize_turbine_state: Initialize turbine state before simulating, default is True.
        initialization_options: Custom input parameters for finding the initial turbine state.
    """
    # Path to resource directory.
    resources = Path(__file__).parent / "resources"

    # Path to model directory.
    model_dir = Path(input_file).parent

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
    fst_file_template = FASTInputFile(f"{os.getcwd()}/{input_file}")
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
            id = f"U_{float(u):05.2f}".replace(".", "d")
            path = f"{os.getcwd()}/{output_dir}/{id}.dat"
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
            f"starting batch {counter}/{math.ceil(len(inflow_files) / float(max_processes))} ...\n"
        )
        counter += 1

        # Iterate over batch and process it in parallel.
        temp_dirs = []
        fst_files = []
        for inflow_file in batch:
            print(f"preparing {Path(inflow_file).stem} ...")
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
            fst_file = FASTInputFile(f"{os.getcwd()}/{input_file}")

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
            if initialize_turbine_state:
                pass
            else:
                # Set initial rotor speed to zero,
                # because this option is normally used by the initial_state function.
                elastodyn_file["RotSpeed"] = 0
            elastodyn_file["OutList"] = [""] + DEFAULT_ELASTODYN_OUT + elastodyn_out
            elastodyn_file.write(fst_file["EDFile"].strip('"'))

            # ServoDyn.
            servodyn_file = FASTInputFile(fst_file["ServoFile"].strip('"'))
            servodyn_file["OutList"] = [""] + DEFAULT_SERVODYN_OUT + servodyn_out
            servodyn_file.write(fst_file["ServoFile"].strip('"'))

            # Write input file.
            fst_file_path = f"{output_dir}/{Path(inflow_file).stem}.fst"
            fst_file.write(fst_file_path)
            fst_files.append(fst_file_path)

        # Run FAST.
        print("\nrunning OpenFAST ...\n")
        tasks = []
        outfiles = []
        for fst_file_path in fst_files:
            stdout = Path(fst_file_path).with_suffix(".out")
            outfiles.append(stdout)
            tasks.append(
                (
                    subprocess.Popen(
                        [fast_exe, fst_file_path],
                        stdout=open(stdout, "w"),
                        stderr=subprocess.STDOUT,
                    ),
                    Path(fst_file_path).stem,
                )
            )

        # Wait for all tasks to finish.
        error = False
        for task, case in tasks:
            return_code = task.wait()
            print(f"task {case} finished with return code {return_code}.")
            if return_code != 0:
                error = True

        print("")

        # Print stdout and stderr.
        if verbose:
            for file in outfiles:
                print(f"########## {Path(file).stem} ##########\n")
                print(open(file, "r").read())

        # Exit due to error or clean up temporary directories.
        if not error:
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
    print("processing output ...")
    failures = []
    for inflow_file in inflow_files:
        try:
            # Load FAST output file.
            output_file = FASTOutputFile(f"{output_dir}/{Path(inflow_file).stem}.outb")

            # Convert to parquet.
            output_file.toDataFrame().rename(columns=RENAME).to_parquet(
                f"{output_dir}/{Path(inflow_file).stem}.parquet"
            )
        except Exception:
            failures.append(Path(inflow_file).stem)

    if len(failures):
        print(f"processing of {len(inflow_files)} cases failed:")
        for failure in failures:
            print(failure)

    # Print completion message.
    if error:
        print("\nOpenFAST terminated, errors occured.\n")
    else:
        print("\nOpenFAST simulation completed successfully.\n")
