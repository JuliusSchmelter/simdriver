import subprocess
import random
from itertools import product, batched
from pathlib import Path
import math

from weio import FASTInputFile


def run_turbsim(
    output_dir: str,
    grid_points_horizontal: int,
    grid_points_vertical: int,
    grid_size_horizontal: float,
    grid_size_vertical: float,
    hub_height: float,
    wind_speed: float | list[float] | None = None,
    turbulence_intensity: str | float | list[float] = "A",
    wind_and_ti: list[tuple[float, float]] | None = None,
    ref_height: float | None = None,
    time_span: int = 660,
    time_step: float = 0.05,
    output_type: str = "bts",
    rand_seed: int | None = None,
    power_law_exponent: float | None = None,
    wind_fields_per_case: int = 1,
    first_wind_field_number: int = 1,
    additional_params: dict = {},
    max_processes: int = 20,
    verbose: bool = False,
):
    """
    Run TurbSim in parallel to generate turbulent wind fields.

    Args:
        output_dir: relative path to output directory.
        grid_points_horizontal: number of grid points in horizontal direction.
        grid_points_vertical: number of grid points in vertical direction.
        grid_size_horizontal: grid size in meters in horizontal direction.
        grid_size_vertical: grid size in meters in vertical direction.
        hub_height: hub height in meters.
        wind_speed: wind speed in m/s, float or a list of floats.
        turbulence_intensity: turbulence intensity in %, float or a list of floats.
        wind_and_ti: alternative input for custom combinations: list of tuples
                     with wind speed and turbulence intensity.
        ref_height: reference height in meters, default is hub height.
        time_span: simulation time in seconds.
        time_step: time step in seconds.
        output_type: output file type, either 'bts' or 'wnd'.
        rand_seed: custom random seed, default generates a new random seed for each case.
        power_law_exponent: power law exponent, None for default value.
        wind_fields_per_case: number of wind fields with different seeds per case.
        first_wind_field_number: number of first wind field when multiple are generated, default is 1.
        additional_params: additional input parameters as dictionary.
        max_processes: maximum number of parallel processes.
        verbose: print stdout and stderr of TurbSim.
    """
    # Path to resource directory.
    resources = Path(__file__).parent / "resources"

    # Create output directory if it does not exist.
    if not Path(output_dir).exists():
        Path(output_dir).mkdir(parents=True)

    # Load template input file.
    file = FASTInputFile(resources / "turbsim_template.inp")

    # Apply scalar parameters.
    file["NumGrid_Y"] = grid_points_horizontal
    file["NumGrid_Z"] = grid_points_vertical
    file["GridWidth"] = grid_size_horizontal
    file["GridHeight"] = grid_size_vertical
    file["HubHt"] = hub_height
    file["AnalysisTime"] = time_span
    file["TimeStep"] = time_step

    if output_type == "bts":
        file["WrADFF"] = True
        file["WrBLFF"] = False
    elif output_type == "wnd":
        file["WrADFF"] = False
        file["WrBLFF"] = True
    else:
        raise ValueError("Unknown output_type. Use 'bts' our 'wnd'.")

    if ref_height is None:
        file["RefHt"] = hub_height
    else:
        file["RefHt"] = ref_height

    if power_law_exponent is not None:
        file["PLExp"] = power_law_exponent

    # Apply additional parameters.
    for key, value in additional_params.items():
        file[key] = value

    # Apply wind speed and turbulence intensity.
    # Make sure wind speed and turbulence intensity are lists.
    if isinstance(wind_speed, int) or isinstance(wind_speed, float):
        wind_speed = [wind_speed]

    if isinstance(turbulence_intensity, int) or isinstance(turbulence_intensity, float):
        turbulence_intensity = [turbulence_intensity]

    # Generate all possible combinations of input parameters.
    inp_files = []
    if wind_and_ti is None:
        wind_and_ti = list(product(wind_speed, turbulence_intensity))

    for i in range(
        first_wind_field_number, first_wind_field_number + wind_fields_per_case
    ):
        for u, ti in wind_and_ti:
            # Apply user-defined seed or generate random seed.
            if rand_seed is None:
                file["RandSeed1"] = random.randint(-2147483648, 2147483647)
            else:
                file["RandSeed1"] = rand_seed

            # Apply wind speed and turbulence intensity.
            file["URef"] = u
            file["IECturbc"] = ti

            # Write TurbSim input file.
            if wind_fields_per_case > 1 or first_wind_field_number != 1:
                id = f"U_{float(u):05.2f}_TI_{float(ti):05.2f}_C_{i:02d}".replace(
                    ".", "d"
                )
            else:
                id = f"U_{float(u):05.2f}_TI_{float(ti):05.2f}".replace(".", "d")
            path = f"{output_dir}/{id}.inp"
            file.write(path)
            inp_files.append(path)

    # Run TurbSim in parallel.
    counter = 1
    for batch in batched(inp_files, max_processes):
        print(
            f"starting batch {counter}/{math.ceil(len(inp_files) / float(max_processes))} ...\n"
        )
        counter += 1

        outfiles = []
        tasks = []
        for inp_file in batch:
            stdout = Path(inp_file).with_suffix(".out")
            outfiles.append(stdout)
            tasks.append(
                (
                    subprocess.Popen(
                        [resources / "TurbSim.exe", inp_file],
                        stdout=open(stdout, "w"),
                        stderr=subprocess.STDOUT,
                    ),
                    Path(inp_file).stem,
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

    # Print completion message.
    if error:
        print("\nTurbSim simulation terminated, errors occured.\n")
    else:
        print("\nTurbSim simulation completed successfully.\n")
