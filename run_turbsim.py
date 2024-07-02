import subprocess
import random
from itertools import product, batched
from pathlib import Path
import math

from openfast_toolbox.io import FASTInputFile


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
    rand_seed: int | None = None,
    # Float or None to use default value.
    power_law_exponent: float | None = None,
    additional_params: dict = {},
    max_processes: int = 32,
) -> list[str]:
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
        path = f"{output_dir}/U_{u}_TI_{ti}.inp"
        file.write(path)
        inp_files.append(path)

    # Run TurbSim in parallel.
    counter = 1
    for batch in batched(inp_files, max_processes):
        print(
            f"\nstarting batch {counter}/{math.ceil(len(inp_files) / float(max_processes))} ...\n"
        )
        counter += 1

        outfiles = []
        tasks = []
        for inp_file in batch:
            stdout = Path(inp_file).with_suffix(".out")
            outfiles.append(stdout)
            tasks.append(
                subprocess.Popen(
                    [resources / "TurbSim.exe", inp_file],
                    stdout=open(stdout, "w"),
                    stderr=subprocess.STDOUT,
                )
            )

        # Wait for all tasks to finish.
        for task in tasks:
            task.wait()

        # Print stdout and stderr.
        for file in outfiles:
            print(f"########## {Path(file).stem} ##########\n")
            print(open(file, "r").read())

    # Print completion message.
    print("\nTurbSim simulation completed.\n")
